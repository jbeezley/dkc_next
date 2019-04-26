#!/usr/bin/env python
from hashlib import sha256
import json
import logging

import click
from elasticsearch import Elasticsearch, NotFoundError
from minio import Minio
import pika

SHA256_BUFFER = 4096
logger = logging.getLogger(__name__)


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
        es_client.update(index='files', id=body['Key'], body={
            'doc': {'sha256': sha},
            'upsert': {'sha256': sha}
        })
        logger.info(f'{body["Key"]} -> {sha}')

    elif event.startswith('s3:ObjectRemoved'):
        try:
            es_client.delete(index='files', id=body['Key'])
            logger.info(f'{body["Key"]} deleted')
        except NotFoundError:
            logger.warn(f'{body["Key"]} not found')
            pass


@click.command()
@click.option('--amqp-host', default='localhost')
@click.option('--amqp-port', default=5672, type=click.INT)
@click.option('--minio-host', default='localhost')
@click.option('--minio-port', default=9000, type=click.INT)
@click.option('--minio-access-key', default='demo')
@click.option('--minio-secret-key', default='secret-key')
@click.option('--elasticsearch-host', default='localhost:9200')
def main(amqp_host, amqp_port, minio_host, minio_port,
         minio_access_key, minio_secret_key, elasticsearch_host):

    minio_client = Minio(
        f'{minio_host}:{minio_port}',
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )
    es_client = Elasticsearch([elasticsearch_host])

    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=amqp_host, port=amqp_port))
    channel = connection.channel()

    channel.exchange_declare(exchange='bucketevents',
                             exchange_type='fanout')

    queue_name = 'checksum-service'
    channel.queue_declare(queue_name, exclusive=True)

    channel.queue_bind(exchange='bucketevents',
                       queue=queue_name)

    def callback(ch, method, properties, body_bytes):
        logger.debug(body_bytes.decode())
        try:
            body = json.loads(body_bytes.decode())
            return process_message(minio_client, es_client, body)
        except Exception:
            logger.exception('processing failed')

    logger.info('Connected')
    channel.basic_consume(queue_name, on_message_callback=callback)
    channel.start_consuming()


if __name__ == '__main__':
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    main()
