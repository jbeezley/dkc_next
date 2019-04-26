import logging
from pathlib import PurePath

from dateutil.parser import parse
from elasticsearch import NotFoundError

SHA256_BUFFER = 4096
logger = logging.getLogger('metadata_service')


def generate_default(record):
    r = record['s3']['object']
    # TODO: Get real mimetype (tika?) for requests from mc client
    return {
        'content-type': r['contentType'],
        'size': r['size'],
        'name': PurePath(r['key']).name,
        'created': parse(record['eventTime'])
    }


def generate_metadata(file_obj, record):
    meta = record['s3']['object'].get('userMetadata', {})  # inject s3 metadata
    return meta


def process_message(minio_client, es_client, body):
    event = body['EventName']
    logger.debug(event)

    if event.startswith('s3:ObjectCreated'):
        bucket, file = body['Key'].split('/', maxsplit=1)

        # TODO: Will this ever contain more than one record?
        assert len(body['Records']) == 1
        record = body['Records'][0]

        file_obj = minio_client.get_object(bucket, file)
        obj = generate_default(record)
        obj['metadata'] = generate_metadata(file_obj, record)

        es_client.update(index='metadata', id=body['Key'], body={
            'doc': obj,
            'upsert': obj
        })
        logger.info(f'{body["Key"]} -> {obj}')

    elif event.startswith('s3:ObjectRemoved'):
        try:
            es_client.delete(index='metadata', id=body['Key'])
            logger.info(f'{body["Key"]} deleted')
        except NotFoundError:
            logger.debug(f'{body["Key"]} already deleted')
