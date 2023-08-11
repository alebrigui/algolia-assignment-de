import os

import pytest
import boto3
from unittest.mock import patch
from moto import mock_s3
from algolia.operators.s3_to_local import S3PublicToLocalOperator

BUCKET = "test_bucket"
KEY = "test_key.csv"
FALSE_KEY = "false_key.csv"
LOCAL_PATH = "/tmp/test_key.csv"
PUBLIC_READ_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{BUCKET}/*"],
        }
    ],
}


@pytest.fixture
def s3_operator():
    return S3PublicToLocalOperator(
        task_id="test_task", s3_bucket=BUCKET, s3_key=KEY, local_path=LOCAL_PATH
    )


@pytest.fixture
def s3_with_moto():
    with mock_s3():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=BUCKET)

        s3_client.put_object(
            Bucket=BUCKET,
            Key=KEY,
            Body="Sample content",
            ACL="public-read",  # Making the object public
        )
        yield s3_client


def test_create_local_dir_if_not_exists(s3_operator):
    with patch("os.makedirs") as mock_makedirs:
        s3_operator._create_local_dir_if_not_exists()
        mock_makedirs.assert_called_once_with("/tmp", exist_ok=True)


def test_pre_execute_creates_directory(s3_operator):
    with patch.object(s3_operator, "_create_local_dir_if_not_exists") as mock_method:
        s3_operator.pre_execute(context={})
        mock_method.assert_called_once()


def test_download_from_public_s3_bucket(s3_operator, s3_with_moto):
    s3_operator.execute(context={})
    assert os.path.exists(LOCAL_PATH)
    with open(LOCAL_PATH, "r") as f:
        content = f.read()
    assert content == "Sample content"


def test_execute_raises_exception_for_missing_file(s3_with_moto):
    s3_operator = S3PublicToLocalOperator(
        task_id="test_task", s3_bucket=BUCKET, s3_key=FALSE_KEY, local_path=LOCAL_PATH
    )

    with pytest.raises(Exception, match=r"File not found: false_key.csv"):
        s3_operator.execute(context={})
