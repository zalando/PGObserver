import re
import boto
from StringIO import StringIO
import yaml
import json


def get_s3_key_as_string(s3_url):
    """ Returns a file from S3 as string. Machine should have access to S3

    Args:
        s3_url (str): in form of 'https://s3-eu-west-1.amazonaws.com/x/y/file.yaml'
        or 'https://s3.eu-central-1.amazonaws.com/x/y/file.yaml'
    Returns:
        str: content of given s3 bucket as string
    """
    m = re.match('https?://s3[.-](.*)\.amazonaws.com/(.*)', s3_url)
    if not m:
        raise Exception('Invalid S3 url: {}'.format(s3_url))
    region_name, bucket_path_with_key_name = m.groups()
    splits = bucket_path_with_key_name.split('/')
    conn = boto.s3.connect_to_region(region_name)
    bucket = conn.get_bucket(bucket_name=splits[0])
    if not bucket:
        raise Exception('S3 bucket not found: {}'.format(splits[0]))
    keys = list(bucket.list(prefix='/'.join(splits[1:])))
    if len(keys) != 1:
        raise Exception('S3 key not found: {}'.format('/'.join(splits[1:])))
    return keys[0].get_contents_as_string()


def get_config_as_dict_from_s3_file(s3_path):
    file_as_str = get_s3_key_as_string(s3_path)
    return yaml.load(StringIO(file_as_str))


def get_client_id_and_secret_from_s3_file(s3_path):
    """
    {
        client_id: "id",
        client_secret: "secret"
    }
    """
    file_json = json.loads(get_s3_key_as_string(s3_path))
    return file_json.get('client_id'), file_json.get('client_secret')
