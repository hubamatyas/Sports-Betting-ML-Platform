import boto3
from mypy_boto3_s3.client import S3Client
from datetime import datetime
import json
import bz2
from enums import AWS


class S3():
    def __init__(self, folder: str):
        """
        Initializes an S3 client to interact with AWS S3, specifically for accessing files within a given folder.

        This constructor sets up the S3 client using credentials stored in AWS.Key, AWS.Secret, and AWS.Bucket.
        These credentials are used to authenticate requests made to AWS S3.

        Parameters:
        - folder (str): The folder within the S3 bucket from which files will be accessed.

        Attributes:
        - folder (str): Folder path within the S3 bucket.
        - key (str): AWS access key ID.
        - secret (str): AWS secret access key.
        - bucket (str): Name of the S3 bucket.
        - s3_client (S3Client): The boto3 S3 client object.
        """

        self.folder = folder
        self.key = AWS.Key.value
        self.secret = AWS.Secret.value
        self.bucket = AWS.Bucket.value
        self.s3_client: S3Client = boto3.client("s3", aws_access_key_id=self.key, aws_secret_access_key=self.secret)

    def get_file_content(self, file_key: str) -> list[str]:
        """
        Fetches and decompresses the content of a specified file from S3, returning its contents as a list of strings.

        Each string in the returned list represents one line from the decompressed file content. If the file
        cannot be decompressed, this function returns None. This might happen due to issues with the uploaded
        files from Kishore's script.

        Parameters:
        - file_key (str): The key (path) of the file within the S3 bucket to fetch.

        Returns:
        - Optional[List[str]]: A list of strings representing the decompressed file content, or None if an error occurs.

        Example:
        - Input: file_key = ""horse-racing/2023/Jan/3/322919/1.23456.bz2""
        - Output: [..."rc":[{"atb":[[1.01,20]]..., ..."rc":[{"atl":[[2.05,12]]...] or None
        """

        response = self.s3_client.get_object(Bucket=self.bucket, Key=file_key)
        compressed_content = response['Body'].read()

        try:
            content = bz2.decompress(compressed_content).decode("utf-8")
            return content.splitlines()
        except Exception as e:
            # get_file_content might return None if the bz2 can't be decompressed
            # and should be skipped (this is a known issue from downloading data from Betfair)
            print(f"Error decompressing file: {file_key}")
            return None


    def fetch_files_from_s3(self) -> list[str]:
        """
        Retrieves the keys of all files located in the specified folder within the S3 bucket.

        This method lists all files present in the initialized folder and collects their keys. If the listing
        is truncated due to the high number of files, it continues to fetch until all files are listed.

        Returns:
        - list[str]: A list of file keys for all files found in the specified folder.

        Example:
        - Input: None if the folder is empty (could be due to incorrect folder path))
        - Output: ["file1.bz2", "file2.bz2", ...]
        """

        response: dict = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self.folder)
        all_files = []
        
        while True:
            # Add the files from the current batch to the all_files list
            all_files.extend(file['Key'] for file in response.get('Contents', []))
            
            # Check if the results are truncated, and if so, continue retrieving more
            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
                response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self.folder, ContinuationToken=continuation_token)
            else:
                print(f"Total number of files retrieved from '{self.folder}': '{len(all_files)}'")
                return all_files