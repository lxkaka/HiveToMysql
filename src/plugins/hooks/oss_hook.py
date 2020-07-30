# -*- coding: utf-8 -*-

from urllib.parse import urlparse

import oss2
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class OSSHook(BaseHook):
    """
    Interact with Aliyun OSS, using the oss2 library.
    """

    def __init__(self, oss_conn_id="oss_default", *args, **kwargs):
        conn = self.get_connection(oss_conn_id)
        self.access_key_id = conn.login
        self.access_key_secret = conn.password
        self.region = conn.host
        self.bucket_name = conn.schema

    @staticmethod
    def parse_oss_url(ossurl):
        parsed_url = urlparse(ossurl)
        if not parsed_url.netloc:
            raise AirflowException(
                'Please provide a bucket_name instead of "%s"' % ossurl
            )
        else:
            bucket_name = parsed_url.netloc
            key = parsed_url.path.strip("/")
            return bucket_name, key

    def get_conn(self):
        auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        bucket = oss2.Bucket(auth, self.region, self.bucket_name)
        return bucket

    def load_string(self, key, string_data, bucket_name=None, replace=False):
        """
        Loads a string to PSS
        This is provided as a convenience to drop a string in OSS. It uses the
        oss2 put_object to ship a file to oss.
        :param string_data: str to set as content for the key.
        :type string_data: str
        :param key: OSS key that will point to the file
        :type key: str
        """
        if urlparse(key).netloc != "":
            (self.bucket_name, key) = self.parse_oss_url(key)

        if not replace and self.get_key(key):
            raise ValueError("The key {key} already exists.".format(key=key))

        if replace:
            self.get_conn().delete_object(key)

        result = self.get_conn().put_object(key=key, data=string_data)
        if result.status != 200:
            raise AirflowException("Upload string file fail")

    def get_key(self, key):
        """
        Check if key exists in remote storage
        :param key: the path to the key
        :type key: str
        """
        if urlparse(key).netloc != "":
            (self.bucket_name, key) = self.parse_oss_url(key)

        return self.get_conn().object_exists(key)

    def read_key(self, key):
        """
        Reads a key from OSS
        :param key: OSS key that will point to the file
        :type key: str
        """
        if urlparse(key).netloc != "":
            (self.bucket_name, key) = self.parse_oss_url(key)

        return self.get_conn().get_object(key).read().decode("utf-8")
