import logging
from pathlib import PurePath

import click
from dateutil.parser import parse
from elasticsearch import NotFoundError
from faker import Faker, providers
from minio_watcher import watch

SHA256_BUFFER = 4096
logger = logging.getLogger(__name__)
fake = Faker()
fake.add_provider(providers.address)
fake.add_provider(providers.geo)
fake.add_provider(providers.profile)


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

    # add some fake data to search on
    latitude, longitude, place, country, timezone = fake.location_on_land()
    meta.update({
        'text': fake.text(),
        'name': fake.name(),
        'latitude': float(latitude),
        'longitude': float(longitude),
        'place': place,
        'country': country,
        'timezone': timezone,
        'profile': fake.profile(),
        'date': fake.date()
    })
    if meta['country'] == 'US':
        state = fake.state_abbr()
        meta['state'] = state
        meta['zip'] = fake.postalcode_in_state(state)

    return meta


def create_handler(body, minio_client, es_client, **kwargs):
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


def delete_handler(body, es_client, **kwargs):
    try:
        es_client.delete(index='metadata', id=body['Key'])
        logger.info(f'{body["Key"]} deleted')
    except NotFoundError:
        logger.debug(f'{body["Key"]} already deleted')


@click.command()
def main():
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    watch('metadata', create_handler, delete_handler)
