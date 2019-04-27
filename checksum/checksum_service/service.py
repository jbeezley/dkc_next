from hashlib import sha256
import logging

import click
from elasticsearch import NotFoundError
from minio_watcher import watch

SHA256_BUFFER = 4096
logger = logging.getLogger(__name__)


def compute_hash(file_obj):
    h = sha256()
    while file_obj.length_remaining > 0:
        h.update(file_obj.read(SHA256_BUFFER))
    return h.hexdigest()


def create_handler(body, minio_client, es_client, **kwargs):
    bucket, file = body['Key'].split('/', maxsplit=1)
    file_obj = minio_client.get_object(bucket, file)
    sha = compute_hash(file_obj)
    es_client.update(index='sha256', id=body['Key'], body={
        'doc': {'sha256': sha},
        'upsert': {'sha256': sha}
    })
    logger.info(f'{body["Key"]} -> {sha}')


def delete_handler(body, es_client, **kwargs):
    try:
        es_client.delete(index='sha256', id=body['Key'])
        logger.info(f'{body["Key"]} deleted')
    except NotFoundError:
        logger.debug(f'{body["Key"]} already deleted')


@click.command()
def main():
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    watch('checksum', create_handler, delete_handler)
