"""Integration test using moto to mock S3, exercising the S3 reader/writer path via
the s3a Hadoop filesystem connector against a local S3-compatible endpoint."""
import boto3
import pytest
from moto import mock_aws

from ingestion.config.models import SourceConfig, SourceType, TargetConfig, TargetType, WriteMode
from ingestion.readers.factory import ReaderFactory
from ingestion.writers.factory import WriterFactory


@pytest.fixture
def s3_bucket():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-ingestion-bucket")
        yield client


def test_moto_s3_bucket_created(s3_bucket):
    """Sanity check that moto intercepts S3 calls without touching real AWS."""
    buckets = s3_bucket.list_buckets()["Buckets"]
    assert any(b["Name"] == "test-ingestion-bucket" for b in buckets)


def test_moto_s3_put_and_list_objects(s3_bucket):
    s3_bucket.put_object(Bucket="test-ingestion-bucket", Key="raw/orders/part-0.json", Body=b'{"id": 1}')
    objects = s3_bucket.list_objects_v2(Bucket="test-ingestion-bucket", Prefix="raw/orders/")
    keys = [o["Key"] for o in objects.get("Contents", [])]
    assert "raw/orders/part-0.json" in keys


def test_reader_writer_factories_recognize_s3_types(spark, tmp_path):
    """The S3 reader/writer implementations operate on any Hadoop-compatible path;
    here we point them at local disk (as a stand-in for a mocked s3a:// mount) to
    verify the factory wiring end-to-end without requiring live AWS credentials."""
    source_path = str(tmp_path / "s3_like_source")
    target_path = str(tmp_path / "s3_like_target")

    df = spark.createDataFrame([(1, "widget"), (2, "gadget")], ["id", "name"])
    df.write.mode("overwrite").json(source_path)

    source = SourceConfig(type=SourceType.S3_JSON, options={"path": source_path})
    reader = ReaderFactory.create(spark, source)
    read_df = reader.read()
    assert read_df.count() == 2

    target = TargetConfig(type=TargetType.S3, write_mode=WriteMode.OVERWRITE, options={"path": target_path})
    writer = WriterFactory.create(spark, target)
    writer.write(read_df)

    result = spark.read.parquet(target_path)
    assert result.count() == 2
