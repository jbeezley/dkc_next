#!/usr/bin/env python
import json
import logging
import os

import click
import dotenv
from elasticsearch import Elasticsearch
from minio import Minio
import pika

from checksum_service.watch import process_message

dotenv.load_dotenv(os.getenv('DOTENV_PATH'))
logger = logging.getLogger('checksum_service')


@click.command()
@click.option('--amqp-host', default=os.getenv('AMQP_HOST', 'localhost'))
@click.option('--amqp-port', default=int(os.getenv('AMQP_PORT', 5672)), type=click.INT)
@click.option('--minio-host', default=os.getenv('MINIO_HOST', 'localhost'))
@click.option('--minio-port', default=int(os.getenv('MINIO_PORT', 9000)), type=click.INT)
@click.option('--minio-access-key', default=os.getenv('MINIO_ACCESS_KEY', 'demo'))
@click.option('--minio-secret-key', default=os.getenv('MINIO_SECRET_KEY', 'secret-key'))
@click.option('--elasticsearch-host', default=os.getenv('ELASTICSEARCH_HOST', 'localhost:9200'))
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
