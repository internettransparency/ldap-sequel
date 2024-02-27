#!/usr/bin/env python3
"""
Objstore handler
"""
__author__ = "Gustavo Luvizotto Cesar"
__email__ = "g.luvizottocesar@utwente.nl"

import base64

import boto3
from botocore.utils import fix_s3_host
import botocore

from utils import calculate_checksum

from config import SIDEKICK_PORT
import credentials as c


class ObjStore(object):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.bucket = self.get_bucket()

    def get_bucket(self):
        s3 = self.get_s3()
        return s3.Bucket(self.bucket_name)

    def get_s3(self):
        # Change timeouts in case we are uploading large files.
        config = botocore.config.Config(connect_timeout=3, read_timeout=99999,
                                        retries={"max_attempts": 3})

        s3 = boto3.resource( # can also replace resource with client if you need that
            's3',
            "nl-utwente-tee",
            aws_access_key_id=c.MINIO_ACCESS_USER,
            aws_secret_access_key=c.MINIO_ACCESS_PASSWORD,
            endpoint_url=f"http://localhost:{SIDEKICK_PORT}",
            config=config
        )

        # next line is needed to prevent some request going to AWS instead of our server
        s3.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)
        return s3

    def get_s3_client(self):
        # Change timeouts in case we are uploading large files.
        config = botocore.config.Config(connect_timeout=3, read_timeout=99999,
                                        retries={"max_attempts": 3})

        s3 = boto3.client( # can also replace resource with client if you need that
            's3',
            "nl-utwente-tee",
            aws_access_key_id=c.MINIO_ACCESS_USER,
            aws_secret_access_key=c.MINIO_ACCESS_PASSWORD,
            endpoint_url=f"http://localhost:{SIDEKICK_PORT}",
            config=config
        )

        # next line is needed to prevent some request going to AWS instead of our server
        s3.meta.events.unregister('before-sign.s3', fix_s3_host)
        return s3

    def get_pages(self, prefix):
        s3_client = self.get_s3_client()
        paginator = s3_client.get_paginator('list_objects_v2')
        return paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

    def upload(self, source_file, target_file):
        src_sha256digest = calculate_checksum(source_file).digest()
        etag = base64.b64encode(src_sha256digest).decode(encoding="utf-8")
        with open(source_file, mode="rb") as f:
            # Upload the object to S3 with the specified ETag
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html#
            self.bucket.put_object(Bucket=self.bucket_name, Key=target_file, Body=f.read(),
                                   ChecksumSHA256=etag)

        etag = [obj.e_tag for obj in self.bucket.objects.filter(Prefix=target_file)][-1].strip('"')
        return etag

    def is_file_already_uploaded(self, filepath: str) -> bool:
        nr_files = self.get_nr_files(filepath)
        if nr_files > 0:
            return True
        else:
            return False

    def get_nr_files(self, objstore_dir: str) -> int:
        obj_list = list(self.bucket.objects.filter(Prefix=objstore_dir))
        return len(obj_list)
