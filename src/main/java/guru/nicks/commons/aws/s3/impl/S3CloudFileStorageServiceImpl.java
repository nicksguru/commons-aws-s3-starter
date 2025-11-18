package guru.nicks.commons.aws.s3.impl;

import guru.nicks.commons.cloud.domain.CloudFile;
import guru.nicks.commons.exception.http.NotFoundException;
import guru.nicks.commons.service.CloudFileStorageService;

import am.ik.yavi.meta.ConstraintArguments;
import jakarta.annotation.Nullable;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
public class S3CloudFileStorageServiceImpl implements CloudFileStorageService {

    @NonNull // Lombok creates runtime nullness check for this own annotation only
    private final S3Client s3Client;

    @SneakyThrows
    @ConstraintArguments
    @Override
    public CloudFile save(@Nullable String userId, InputStream inputStream, String filename, MediaType contentType,
            Map<String, ?> metadata) {
        checkNotNull(inputStream, _S3CloudFileStorageServiceImplSaveArgumentsMeta.INPUTSTREAM.name());
        checkNotBlank(filename, _S3CloudFileStorageServiceImplSaveArgumentsMeta.FILENAME.name());
        checkNotNull(contentType, _S3CloudFileStorageServiceImplSaveArgumentsMeta.CONTENTTYPE.name());

        // parse and validate filename which is 's3://bucket/path/to/file'
        final var s3Uri = s3Client.utilities().parseUri(URI.create(filename));
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

        // calculate content checksum (can't read more than Integer.MAX_VALUE bytes!)
        byte[] content;
        try {
            content = IOUtils.toByteArray(inputStream);
        } catch (IOException e) {
            throw new IllegalArgumentException("Stream failure or stream too large: " + e.getMessage(), e);
        }

        s3Metadata.put(CloudFileStorageService.METADATA_CHECKSUM, computeChecksum(content));

        var request = PutObjectRequest.builder()
                .bucket(s3Uri.bucket().orElseThrow())
                .key(s3Uri.key().orElseThrow())
                .metadata(s3Metadata)
                .contentType(contentType.toString())
                .build();

        s3Client.putObject(request, RequestBody.fromBytes(content));
        return getByFilename(filename);
    }

    @Override
    public Optional<CloudFile> findByFilename(String filename) {
        // parse and validate filename which is 's3://bucket/path/to/file'
        var s3Uri = s3Client.utilities().parseUri(URI.create(filename));

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
        var s3Uri = s3Client.utilities().parseUri(URI.create(id));

        var request = GetObjectRequest.builder()
                .bucket(s3Uri.bucket().orElseThrow())
                .key(s3Uri.key().orElseThrow())
                .build();

        try {
            return s3Client.getObjectAsBytes(request).asInputStream();
        } catch (AwsServiceException e) {
            throw new NotFoundException(e.getMessage(), e);
        }
    }

    @Override
    public List<CloudFile> listFiles(String path) {
        // parse and validate filename which is 's3://bucket/path/to/file'
        var s3Uri = s3Client.utilities().parseUri(URI.create(path));

        var request = ListObjectsV2Request.builder()
                .bucket(s3Uri.bucket().orElseThrow())
                .prefix(path)
                .build();

        return s3Client.listObjectsV2Paginator(request)
                .stream()
                .map(ListObjectsV2Response::contents)
                // page contents (list of S3Object)
                .flatMap(List::stream)
                .map(s3Obj -> fetchFileMetadata(s3Uri.bucket().orElseThrow(), s3Obj.key()))
                .toList();
    }

    @Override
    public void deleteById(String id) {
        // parse and validate filename which is 's3://bucket/path/to/file'
        var s3Uri = s3Client.utilities().parseUri(URI.create(id));

        var request = DeleteObjectRequest.builder()
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
        var uri = S3Uri.builder()
                // TODO: S3Uri constructor fails without the URI, but what's the reason to construct manually -
                // the idea is to make the builder do it?
                .uri(URI.create("s3://" + bucketName + "/" + key))
                .bucket(bucketName)
                .key(key)
                .build();

        var request = HeadObjectRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.bucket().orElseThrow())
                .build();

        HeadObjectResponse objectHead;
        try {
            objectHead = s3Client.headObject(request);
        } catch (AwsServiceException e) {
            throw new NotFoundException(e.getMessage(), e);
        }

        // never null
        Map<String, String> metadata = objectHead.metadata();

        return CloudFile.builder()
                .id(uri.uri().toString())
                .filename(uri.uri().toString())
                .lastModified(objectHead.lastModified())
                .contentType(MediaType.valueOf(objectHead.contentType()))
                .size(objectHead.contentLength())
                .userId(metadata.get(CloudFileStorageService.METADATA_USER_ID))
                .checksum(metadata.get(CloudFileStorageService.METADATA_CHECKSUM))
                .build();
    }

}
