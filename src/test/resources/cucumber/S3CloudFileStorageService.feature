@aws #@disabled
Feature: S3 Cloud File Storage Service
  Cloud file storage service implementation using AWS S3

  Scenario Outline: Saving a file to S3
    Given file user ID is "<userId>"
    And file metadata contains:
      | key       | value     |
      | attribute | testValue |
    And a file with content "<content>" and bucket "<bucket>" and filename "<filename>" and content type "<contentType>"
    When the file is saved in S3
    Then no exception should be thrown
    And the saved file should have the following properties:
      | property    | value                    |
      | id          | s3://<bucket>/<filename> |
      | filename    | s3://<bucket>/<filename> |
      | userId      | <userId>                 |
      | contentType | <contentType>            |
    Examples:
      | content     | bucket  | filename        | contentType      | userId  |
      | Hello World | bucket1 | path/file1.txt  | text/plain       | user123 |
      | Test Data   | bucket2 | path/file2.json | application/json | user456 |

  Scenario: Finding a file by filename
    Given a file exists with filename "s3://bucket/path/file.txt"
    When the file is found by filename
    Then the file should be found
    And the file should have ID "s3://bucket/path/file.txt"

  Scenario: Finding a non-existent file by filename
    Given a file does not exist with filename "s3://bucket/path/nonexistent.txt"
    When the file is found by filename
    Then the file should not be found

  Scenario: Getting input stream for a file
    Given a file exists with filename "s3://bucket/path/file.txt"
    When the input stream is requested for the file
    Then no exception should be thrown
    And the input stream should contain "file content"

  Scenario: Listing files in a directory
    Given files exist in directory "s3://bucket/path/"
    When files are listed in directory "s3://bucket/path/"
    Then the file list should contain 2 files
    And the file list should contain a file with id "s3://bucket/path/file1.txt"
    And the file list should contain a file with id "s3://bucket/path/file2.txt"

  Scenario: Deleting a file
    Given a file exists with filename "s3://bucket/path/file.txt"
    When the file is deleted
    Then no exception should be thrown
