package guru.nicks.commons.aws.s3.impl;

import guru.nicks.commons.cloud.domain.CloudFile;
import guru.nicks.commons.exception.http.NotFoundException;
import guru.nicks.commons.service.CloudFileStorageService;

import am.ik.yavi.meta.ConstraintArguments;
import jakarta.annotation.Nullable;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.MediaType;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static guru.nicks.commons.validation.dsl.ValiDsl.checkNotBlank;
import static guru.nicks.commons.validation.dsl.ValiDsl.checkNotNull;

/**
 * AWS S3-based implementation. For S3, the object ID in all methods below is the same as the filename and is actually a
 * URI in the form 's3://bucket/path/to/file'.
 * <p>
 * See also <a href="https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/java_s3_code_examples.html"
 * >official examples</a>.
 */
@RequiredArgsConstructor
@Slf4j
public class S3CloudFileStorageServiceImpl implements CloudFileStorageService {

    /**
     * Computed once: whether the default filesystem exposes POSIX attributes (false on Windows, where JDK temp file
     * defaults are already owner-only).
     */
    private static final boolean POSIX_ATTRIBUTES_SUPPORTED =
            FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    @NonNull // Lombok creates runtime nullness check for this own annotation only
    private final S3Client s3Client;

    /**
     * Isolated spool directory, created lazily on first upload; atomic because save() may run concurrently.
     */
    private final AtomicReference<Path> spoolDirectory = new AtomicReference<>();

    /**
     * Builds a listing entry out of the data the S3 listing response already provides. No extra HEAD request is made,
     * therefore {@code contentType}, {@code userId} and {@code checksum} (which the listing response lacks) are left
     * {@code null} - call {@link #findByFilename(String)} to fetch them.
     *
     * @param bucket   bucket name
     * @param s3Object listed object
     * @return listing entry
     */
    private static CloudFile buildListedFile(String bucket, S3Object s3Object) {
        String uri = toUri(bucket, s3Object.key());
        return CloudFile.builder()
                .id(uri)
                .filename(uri)
                .lastModified(s3Object.lastModified())
                .size(s3Object.size())
                .build();
    }

    /**
     * Translates S3 'not found' failures (HTTP 404 / 'NoSuchKey' error code) into {@link NotFoundException}, rethrowing
     * everything else as is: mapping all AWS errors to 'not found' would mask real problems such as missing permissions
     * or connectivity issues.
     *
     * @param e AWS error
     * @return exception to throw when the object is missing
     */
    private static NotFoundException mapToNotFoundException(AwsServiceException e) {
        if ((e.statusCode() != 404) && !isNoSuchKeyError(e)) {
            throw e;
        }

        return new NotFoundException(e);
    }

    /**
     * Same as {@link #mapToNotFoundException(AwsServiceException)}, but the returned exception message carries the
     * object URI, which the AWS error itself does not always include.
     *
     * @param e   AWS error
     * @param uri object URI to include in the error message
     * @return exception to throw when the object is missing
     */
    private static NotFoundException mapToNotFoundException(AwsServiceException e, String uri) {
        if ((e.statusCode() != 404) && !isNoSuchKeyError(e)) {
            throw e;
        }

        return new NotFoundException("No such object: " + uri, e);
    }

    /**
     * @return check result; error details may be absent, for example when S3 answers a HEAD request with no body
     */
    private static boolean isNoSuchKeyError(AwsServiceException e) {
        return (e.awsErrorDetails() != null)
                && "NoSuchKey".equals(e.awsErrorDetails().errorCode());
    }

    /**
     * Creates a spool file with owner-only 'rw-------' POSIX permissions applied atomically at creation time, so the
     * file is never observable with wider permissions (no create-then-chmod window for an attacker to race).
     * Filesystems without a POSIX view (e.g. Windows) fall back to plain JDK creation, whose defaults are owner-only
     * there.
     *
     * @param directory parent spool directory
     * @return created spool file
     * @throws IOException file creation failure
     */
    private static Path createSpoolFile(Path directory) throws IOException {
        if (!POSIX_ATTRIBUTES_SUPPORTED) {
            return Files.createTempFile(directory, "commons-s3-upload-", ".tmp");
        }

        return Files.createTempFile(directory, "commons-s3-upload-", ".tmp",
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    }

    /**
     * Creates the isolated spool directory with owner-only 'rwx------' permissions, applying the same non-POSIX
     * fallback as {@link #createSpoolFile(Path)}.
     *
     * @return created spool directory
     * @throws IOException directory creation failure
     */
    private static Path createSpoolDirectory() throws IOException {
        if (!POSIX_ATTRIBUTES_SUPPORTED) {
            return Files.createTempDirectory("commons-s3-spool-");
        }

        return Files.createTempDirectory("commons-s3-spool-",
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    }

    /**
     * @return SHA-256 digest which, unlike {@link MessageDigest#getInstance(String)}, never fails because SHA-256 is
     *         mandated by the JVM specification
     */
    private static MessageDigest getSha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * Removes the temporary file, ignoring failures because it's a best-effort cleanup which must not mask the
     * operation result.
     *
     * @param tempFile file to remove
     */
    private static void deleteQuietly(Path tempFile) {
        try {
            Files.deleteIfExists(tempFile);
        } catch (IOException e) {
            log.warn("Failed to remove temporary file '{}'", tempFile, e);
        }
    }

    /**
     * @return canonical object URI in the 's3://bucket/key' form
     */
    private static String toUri(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }

    /**
     * Returns the isolated spool directory, creating it lazily on first upload. Spooling inside a dedicated 'rwx------'
     * directory keeps spooled uploads out of the shared world-writable temp directory entirely, so no other user can
     * pre-create entries there, hijack names, or read spooled content even if file-level permissions were somehow
     * lost.
     * <p>
     * The directory is cached for the service lifetime instead of being recreated per upload: the creation cost is paid
     * once, concurrent first uploads are published via a lock-free CAS, and teardown stays deterministic.
     *
     * @return isolated spool directory
     * @throws IOException spool directory creation failure
     */
    private Path getSpoolDirectory() throws IOException {
        // fast path: the directory is already published
        Path dir = spoolDirectory.get();
        if (dir != null) {
            return dir;
        }

        // randomized name + O_EXCL semantics: an attacker can neither pre-create nor claim it
        Path created = createSpoolDirectory();

        // lock-free publish: exactly one CAS(null -> created) wins, so exactly one directory is published and
        // registered for exit-cleanup; each loser's own directory is still empty and gets deleted immediately
        // below, so no duplicate directories are leaked and no locks are needed
        if (spoolDirectory.compareAndSet(null, created)) {
            // cached directory is intentionally not deleted per call; each spool file removes itself in
            // save()'s finally block, so the directory is empty at JVM exit and deleteOnExit (or container
            // teardown) collects it
            created.toFile().deleteOnExit();
            return created;
        }

        // CAS loser: another thread won the race, so best-effort remove the just-created empty duplicate
        try {
            Files.delete(created);
        } catch (IOException e) {
            log.warn("Failed to remove duplicate spool directory '{}'", created, e);
        }

        // non-null because the CAS failed only after another thread published a directory
        return spoolDirectory.get();
    }

    /**
     * Copies the stream into a temporary file computing its SHA-256 checksum on the fly, so that files of arbitrary
     * size can be uploaded without buffering them in memory.
     * <p>
     * Temp-directory hardening: the file is created inside the isolated owner-only spool directory (see
     * {@link #getSpoolDirectory()}) with 'rw-------' permissions applied atomically at creation time, so there is no
     * window in which spooled content is readable by other users.
     * <p>
     * Name-guessing races are not exploitable either: {@link Files#createTempFile(String, String, FileAttribute[])}
     * combines a randomized name with O_EXCL (CREATE_NEW) semantics, so a pre-existing attacker file or symlink under
     * the guessed name makes the JDK fail and retry with a fresh random name instead of opening or clobbering it. The
     * residual JDK defaults (umask-derived permissions, shared directory) are exactly what the owner-only attributes
     * and the isolated spool directory eliminate.
     *
     * @param inputStream content to spool
     * @return spooled content with its checksum
     * @throws IOException stream failure
     */
    private SpooledContent spoolContent(InputStream inputStream) throws IOException {
        Path tempFile = createSpoolFile(getSpoolDirectory());
        MessageDigest digest = getSha256Digest();

        try (var outputStream = Files.newOutputStream(tempFile);
                var digestInputStream = new DigestInputStream(inputStream, digest)) {
            digestInputStream.transferTo(outputStream);
        } catch (IOException e) {
            // the partially written file is useless - remove it without masking the original failure
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }

            throw e;
        }

        return new SpooledContent(tempFile, HexFormat.of().formatHex(digest.digest()));
    }

    @ConstraintArguments
    @Override
    public CloudFile save(@Nullable String userId, InputStream inputStream, String filename, MediaType contentType,
            Map<String, ?> metadata) {
        checkNotNull(inputStream, _S3CloudFileStorageServiceImplSaveArgumentsMeta.INPUTSTREAM.name());
        checkNotBlank(filename, _S3CloudFileStorageServiceImplSaveArgumentsMeta.FILENAME.name());
        checkNotNull(contentType, _S3CloudFileStorageServiceImplSaveArgumentsMeta.CONTENTTYPE.name());

        // parse and validate filename which is 's3://bucket/path/to/file'
        final S3Uri s3Uri = s3Client.utilities().parseUri(URI.create(filename));
        var s3Metadata = new HashMap<String, String>();

        if (MapUtils.isNotEmpty(metadata)) {
            // S3 permits string values only
            Map<String, String> mapWithStringValues = metadata.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() != null)
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
            s3Metadata.putAll(mapWithStringValues);
        }

        if (StringUtils.isNotBlank(userId)) {
            s3Metadata.put(CloudFileStorageService.METADATA_USER_ID, userId);
        }

        // spool the content to a temporary file while computing its checksum: memory consumption stays constant no
        // matter how large the file is, and the checksum must be known before the upload starts because it travels in
        // request metadata
        SpooledContent spooledContent;
        try {
            spooledContent = spoolContent(inputStream);
        } catch (IOException e) {
            throw new IllegalArgumentException("Stream failure: " + e.getMessage(), e);
        }

        try {
            s3Metadata.put(CloudFileStorageService.METADATA_CHECKSUM, spooledContent.checksum());

            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(s3Uri.bucket().orElseThrow())
                    .key(s3Uri.key().orElseThrow())
                    .metadata(s3Metadata)
                    .contentType(contentType.toString())
                    .build();

            s3Client.putObject(request, RequestBody.fromFile(spooledContent.file()));
            return getByFilename(filename);
        } finally {
            deleteQuietly(spooledContent.file());
        }
    }

    @Override
    public Optional<CloudFile> findByFilename(String filename) {
        // parse and validate filename which is 's3://bucket/path/to/file'
        S3Uri s3Uri = s3Client.utilities().parseUri(URI.create(filename));

        try {
            return Optional.of(fetchFileMetadata(s3Uri.bucket().orElseThrow(), s3Uri.key().orElseThrow()));
        } catch (NotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<CloudFile> findById(String id) {
        return findByFilename(id);
    }

    @Override
    public InputStream getInputStream(String id) {
        // parse and validate filename which is 's3://bucket/path/to/file'
        S3Uri s3Uri = s3Client.utilities().parseUri(URI.create(id));

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(s3Uri.bucket().orElseThrow())
                .key(s3Uri.key().orElseThrow())
                .build();

        // streamed as is, without loading the whole object into memory
        try {
            return s3Client.getObject(request);
        } catch (AwsServiceException e) {
            throw mapToNotFoundException(e);
        }
    }

    @Override
    public List<CloudFile> listFiles(String path) {
        // parse and validate filename which is 's3://bucket/path/to/file'
        S3Uri s3Uri = s3Client.utilities().parseUri(URI.create(path));
        String bucket = s3Uri.bucket().orElseThrow();

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(s3Uri.key().orElseThrow())
                .build();

        return s3Client.listObjectsV2Paginator(request)
                .stream()
                .map(ListObjectsV2Response::contents)
                // page contents (list of S3Object)
                .flatMap(List::stream)
                // build entries right from the listing response: fetching each object's metadata separately would
                // mean one HEAD request per object (N+1 problem)
                .map(s3Object -> buildListedFile(bucket, s3Object))
                .toList();
    }

    @Override
    public void deleteById(String id) {
        // parse and validate filename which is 's3://bucket/path/to/file'
        S3Uri s3Uri = s3Client.utilities().parseUri(URI.create(id));

        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(s3Uri.bucket().orElseThrow())
                .key(s3Uri.key().orElseThrow())
                .build();

        // no error when deleting a non-existing object
        s3Client.deleteObject(request);
    }

    /**
     * Fetches object metadata.
     *
     * @param bucketName bucket name
     * @param key        object key within the bucket
     * @return metadata
     * @throws NotFoundException no such entry or no access to it
     */
    private CloudFile fetchFileMetadata(String bucketName, String key) {
        // parse and validate filename which is 's3://bucket/path/to/file'
        S3Uri uri = S3Uri.builder()
                // TODO: S3Uri constructor fails without the URI, but what's the reason to construct manually -
                // the idea is to make the builder do it?
                .uri(URI.create(toUri(bucketName, key)))
                .bucket(bucketName)
                .key(key)
                .build();

        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .build();

        HeadObjectResponse objectHead;
        try {
            objectHead = s3Client.headObject(request);
        } catch (AwsServiceException e) {
            throw mapToNotFoundException(e, uri.uri().toString());
        }

        // never null
        Map<String, String> metadata = objectHead.metadata();

        return CloudFile.builder()
                .id(uri.uri().toString())
                .filename(uri.uri().toString())
                .lastModified(objectHead.lastModified())
                // content type may be absent, fall back to a generic binary type
                .contentType(Optional
                        .ofNullable(objectHead.contentType())
                        .map(MediaType::valueOf)
                        .orElse(MediaType.APPLICATION_OCTET_STREAM))
                .size(objectHead.contentLength())
                .userId(metadata.get(CloudFileStorageService.METADATA_USER_ID))
                .checksum(metadata.get(CloudFileStorageService.METADATA_CHECKSUM))
                .build();
    }

    /**
     * Content spooled to a temporary file, with its SHA-256 checksum.
     */
    private record SpooledContent(

            Path file,
            String checksum) {
    }

}
