from hashlib import sha256
import logging

from elasticsearch import NotFoundError

SHA256_BUFFER = 4096
logger = logging.getLogger('checksum_service.watch')


def compute_hash(file_obj):
    h = sha256()
    while file_obj.length_remaining > 0:
        h.update(file_obj.read(SHA256_BUFFER))
    return h.hexdigest()


def process_message(minio_client, es_client, body):
    event = body['EventName']
    logger.debug(event)

    if event.startswith('s3:ObjectCreated'):
        bucket, file = body['Key'].split('/', maxsplit=1)
        file_obj = minio_client.get_object(bucket, file)
        sha = compute_hash(file_obj)
        es_client.update(index='sha256', id=body['Key'], body={
            'doc': {'sha256': sha},
            'upsert': {'sha256': sha}
        })
        logger.info(f'{body["Key"]} -> {sha}')

    elif event.startswith('s3:ObjectRemoved'):
        try:
            es_client.delete(index='sha256', id=body['Key'])
            logger.info(f'{body["Key"]} deleted')
        except NotFoundError:
            logger.debug(f'{body["Key"]} already deleted')
