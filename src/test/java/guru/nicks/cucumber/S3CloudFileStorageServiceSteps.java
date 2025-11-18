package guru.nicks.cucumber;

import guru.nicks.commons.aws.s3.impl.S3CloudFileStorageServiceImpl;
import guru.nicks.commons.cloud.domain.CloudFile;
import guru.nicks.commons.cucumber.world.TextWorld;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.DataTableType;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RequiredArgsConstructor
public class S3CloudFileStorageServiceSteps {

    // DI
    private final TextWorld textWorld;

    @Mock
    private S3Client s3Client;
    @Mock
    private S3Utilities s3Utilities;
    @Mock
    private ListObjectsV2Iterable listObjectsV2Iterable;
    private AutoCloseable closeableMocks;

    private S3CloudFileStorageServiceImpl s3CloudFileStorageService;
    private String content;
    private String filename;
    private MediaType contentType;
    private String fileUserId;
    private Map<String, String> metadata;

    private CloudFile savedFile;
    private Optional<CloudFile> foundFile;
    private InputStream fileInputStream;
    private List<CloudFile> fileList;

    @Before
    public void beforeEachScenario() {
        closeableMocks = MockitoAnnotations.openMocks(this);

        when(s3Client.utilities())
                .thenReturn(s3Utilities);
        s3CloudFileStorageService = new S3CloudFileStorageServiceImpl(s3Client);
    }

    @After
    public void afterEachScenario() throws Exception {
        closeableMocks.close();
    }

    @DataTableType
    public FileProperty createFileProperty(Map<String, String> entry) {
        return FileProperty.builder()
                .property(entry.get("property"))
                .value(entry.get("value"))
                .build();
    }

    @DataTableType
    public MetadataEntry createMetadataEntry(Map<String, String> entry) {
        return MetadataEntry.builder()
                .key(entry.get("key"))
                .value(entry.get("value"))
                .build();
    }

    @Given("a file with content {string} and bucket {string} and filename {string} and content type {string}")
    public void aFileWithContentAndFilenameAndContentType(String content, String bucket,
            String filename, String contentType) {
        this.content = content;
        this.filename = filename;
        this.contentType = MediaType.valueOf(contentType);

        // mock S3Uri parsing
        var mockS3Uri = mock(S3Uri.class);
        when(s3Utilities.parseUri(any(URI.class)))
                .thenReturn(mockS3Uri);
        when(mockS3Uri.bucket())
                .thenReturn(Optional.of(bucket));
        when(mockS3Uri.key())
                .thenReturn(Optional.of(filename));

        // mock HeadObjectResponse for the saved file including its metadata ALREADY specified earlier (userId+metadata)
        var enrichedMetadata = new HashMap<>(metadata);
        enrichedMetadata.put("userId", fileUserId);

        var headObjectResponse = mock(HeadObjectResponse.class);
        when(headObjectResponse.contentType())
                .thenReturn(contentType);
        when(headObjectResponse.lastModified())
                .thenReturn(Instant.now());
        when(headObjectResponse.contentLength())
                .thenReturn((long) content.length());
        when(headObjectResponse.metadata())
                .thenReturn(enrichedMetadata);

        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(headObjectResponse);
    }

    @Given("file user ID is {string}")
    public void fileUserIdIs(String fileUserId) {
        this.fileUserId = fileUserId;
    }

    @Given("file metadata contains:")
    public void fileMetadataContains(List<MetadataEntry> metadataEntries) {
        metadata = new HashMap<>();

        for (MetadataEntry entry : metadataEntries) {
            metadata.put(entry.getKey(), entry.getValue());
        }
    }

    @When("the file is saved in S3")
    public void theFileIsSavedInS3() {
        var inputStream = new ByteArrayInputStream(content.getBytes());

        textWorld.setLastException(catchThrowable(() ->
                savedFile = s3CloudFileStorageService.save(fileUserId, inputStream, filename, contentType, metadata)
        ));
    }

    @Then("the saved file should have the following properties:")
    public void theSavedFileShouldHaveTheFollowingProperties(List<FileProperty> properties) {
        for (FileProperty property : properties) {
            switch (property.getProperty()) {
                case "id" -> assertThat(savedFile.getId())
                        .as("file ID")
                        .isEqualTo(property.getValue());
                case "filename" -> assertThat(savedFile.getFilename())
                        .as("filename")
                        .isEqualTo(property.getValue());
                case "userId" -> assertThat(savedFile.getUserId())
                        .as("userId")
                        .isEqualTo(property.getValue());
                case "contentType" -> assertThat(savedFile.getContentType())
                        .as("contentType")
                        .hasToString(property.getValue());
                default -> throw new IllegalArgumentException("Unknown property: " + property.getProperty());
            }
        }

        // verify S3 client was called with correct parameters
        var requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));

        PutObjectRequest capturedRequest = requestCaptor.getValue();
        assertThat(capturedRequest.contentType())
                .as("request contentType")
                .isEqualTo(contentType.toString());
    }

    @Given("a file exists with filename {string}")
    public void aFileExistsWithFilename(String filename) {
        this.filename = filename;

        // mock S3Uri parsing
        var mockS3Uri = mock(S3Uri.class);
        when(s3Utilities.parseUri(any(URI.class)))
                .thenReturn(mockS3Uri);
        when(mockS3Uri.bucket())
                .thenReturn(Optional.of("bucket"));
        when(mockS3Uri.key())
                .thenReturn(Optional.of("path/file.txt"));
        when(mockS3Uri.toString())
                .thenReturn(filename);

        // mock HeadObjectResponse for the existing file
        var headObjectResponse = mock(HeadObjectResponse.class);
        when(headObjectResponse.contentType())
                .thenReturn("text/plain");
        when(headObjectResponse.lastModified())
                .thenReturn(Instant.now());
        when(headObjectResponse.contentLength())
                .thenReturn(12L);
        when(headObjectResponse.metadata())
                .thenReturn(Map.of("userId", "user123", "checksum", "abc123"));

        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(headObjectResponse);
    }

    @Given("a file does not exist with filename {string}")
    public void aFileDoesNotExistWithFilename(String filename) {
        this.filename = filename;

        // mock S3Uri parsing
        var mockS3Uri = mock(S3Uri.class);
        when(s3Utilities.parseUri(any(URI.class)))
                .thenReturn(mockS3Uri);
        when(mockS3Uri.bucket())
                .thenReturn(Optional.of("bucket"));
        when(mockS3Uri.key())
                .thenReturn(Optional.of("path/nonexistent.txt"));

        // mock exception for the non-existent file
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(AwsServiceException.builder().message("Not Found").build());
    }

    @When("the file is found by filename")
    public void theFileIsFoundByFilename() {
        foundFile = s3CloudFileStorageService.findByFilename(filename);
    }

    @Then("the file should be found")
    public void theFileShouldBeFound() {
        assertThat(foundFile)
                .as("found file")
                .isPresent();
    }

    @Then("the file should not be found")
    public void theFileShouldNotBeFound() {
        assertThat(foundFile)
                .as("found file")
                .isEmpty();
    }

    @Then("the file should have ID {string}")
    public void theFileShouldHaveId(String expectedId) {
        assertThat(foundFile)
                .as("foundFile")
                .isPresent();

        assertThat(foundFile.get().getId())
                .as("foundFile ID")
                .isEqualTo(expectedId);
    }

    @When("the input stream is requested for the file")
    public void theInputStreamIsRequestedForTheFile() {
        // mock response bytes for getObjectAsBytes
        var responseBytes = mock(ResponseBytes.class);
        when(responseBytes.asInputStream())
                .thenReturn(new ByteArrayInputStream("file content".getBytes()));

        when(s3Client.getObjectAsBytes(any(GetObjectRequest.class)))
                .thenReturn(responseBytes);

        textWorld.setLastException(catchThrowable(() ->
                fileInputStream = s3CloudFileStorageService.getInputStream(filename)
        ));
    }

    @Then("the input stream should contain {string}")
    public void theInputStreamShouldContain(String expectedContent) throws Exception {
        byte[] bytes = fileInputStream.readAllBytes();
        String str = new String(bytes);

        assertThat(str)
                .as("input stream content")
                .isEqualTo(expectedContent);
    }

    @Given("files exist in directory {string}")
    public void filesExistInDirectory(String directory) {
        // mock S3Uri parsing
        var mockS3Uri = mock(S3Uri.class);
        when(s3Utilities.parseUri(any(URI.class)))
                .thenReturn(mockS3Uri);
        when(mockS3Uri.bucket())
                .thenReturn(Optional.of("bucket"));
        when(mockS3Uri.key())
                .thenReturn(Optional.of("path/"));

        // mock ListObjectsV2Paginator
        when(s3Client.listObjectsV2Paginator(any(ListObjectsV2Request.class)))
                .thenReturn(listObjectsV2Iterable);

        // mock S3Objects for the list
        var s3Object1 = mock(S3Object.class);
        when(s3Object1.key())
                .thenReturn("path/file1.txt");

        var s3Object2 = mock(S3Object.class);
        when(s3Object2.key())
                .thenReturn("path/file2.txt");

        // mock ListObjectsV2Response
        var listObjectsV2Response = mock(ListObjectsV2Response.class);
        when(listObjectsV2Response.contents())
                .thenReturn(List.of(s3Object1, s3Object2));

        // mock stream of ListObjectsV2Response
        when(listObjectsV2Iterable.stream())
                .thenReturn(Stream.of(listObjectsV2Response));

        // mock HeadObjectResponse for each file
        var headObjectResponse = mock(HeadObjectResponse.class);
        when(headObjectResponse.contentType())
                .thenReturn("text/plain");
        when(headObjectResponse.lastModified())
                .thenReturn(Instant.now());
        when(headObjectResponse.contentLength())
                .thenReturn(12L);
        when(headObjectResponse.metadata())
                .thenReturn(Map.of("userId", "user123", "checksum", "abc123"));

        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(headObjectResponse);
    }

    @When("files are listed in directory {string}")
    public void filesAreListedInDirectory(String directory) {
        fileList = s3CloudFileStorageService.listFiles(directory);
    }

    @Then("the file list should contain {int} files")
    public void theFileListShouldContainFiles(int expectedCount) {
        assertThat(fileList)
                .as("file list")
                .hasSize(expectedCount);
    }

    @Then("the file list should contain a file with id {string}")
    public void theFileListShouldContainAFileWithId(String fileId) {
        assertThat(fileList)
                .as("file list")
                .anyMatch(file -> file.getId().equals(fileId));
    }

    @When("the file is deleted")
    public void theFileIsDeleted() {
        textWorld.setLastException(catchThrowable(() ->
                s3CloudFileStorageService.deleteById(filename)
        ));

        // verify delete was called
        verify(s3Client).deleteObject(any(DeleteObjectRequest.class));
    }

    @Value
    @Builder
    public static class FileProperty {

        String property;
        String value;

    }

    @Value
    @Builder
    public static class MetadataEntry {

        String key;
        String value;

    }

}
