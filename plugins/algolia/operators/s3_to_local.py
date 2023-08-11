import os

import botocore
from airflow.models import BaseOperator
import boto3


class S3PublicToLocalOperator(BaseOperator):
    """
    Downloads a file from S3 to the local filesystem.
    """

    template_fields = (
        "s3_key",
        "local_path",
    )

    def __init__(
        self: object, s3_bucket: str, s3_key: str, local_path: str, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.local_path = local_path

    def _create_local_dir_if_not_exists(self):
        os.makedirs(os.path.dirname(self.local_path), exist_ok=True)

    def pre_execute(self, context: dict):
        self._create_local_dir_if_not_exists()

    def execute(self, context: dict):
        self.log.info(f"Downloading {self.s3_key} from {self.s3_bucket}")

        s3 = boto3.client(
            "s3", config=boto3.session.Config(signature_version=botocore.UNSIGNED)
        )

        try:
            s3.head_object(Bucket=self.s3_bucket, Key=self.s3_key)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                raise Exception(f"File not found: {self.s3_key}")
            else:
                raise e

        s3.download_file(self.s3_bucket, self.s3_key, self.local_path)

        self.log.info(f"Download complete: {self.local_path}")
