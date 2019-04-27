import json
import logging
import os

import dotenv
from elasticsearch import Elasticsearch
from minio import Minio
import pika


dotenv.load_dotenv(os.getenv('DOTENV_PATH'))
logger = logging.getLogger('minio_watcher')

# TODO: use a real config system
minio_host = os.getenv('MINIO_HOST', 'localhost')
minio_port = int(os.getenv('MINIO_PORT', 9000))
minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'demo')
minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'secret-key')

amqp_host = os.getenv('AMQP_HOST', 'localhost')
amqp_port = int(os.getenv('AMQP_PORT', 5672))

elasticsearch_hosts = os.getenv('ELASTICSEARCH_HOST', 'localhost:9200').split(',')


def get_minio_client():
    return Minio(
        f'{minio_host}:{minio_port}',
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )


def get_elasticsearch_client():
    return Elasticsearch(elasticsearch_hosts)


def watch(queue_name, create_handler, delete_handler):
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    minio_client = get_minio_client()
    es_client = get_elasticsearch_client()

    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=amqp_host, port=amqp_port))
    channel = connection.channel()

    channel.exchange_declare(exchange='bucketevents',
                             exchange_type='fanout')

    channel.queue_declare(queue_name, exclusive=True)

    channel.queue_bind(exchange='bucketevents',
                       queue=queue_name)

    def callback(ch, method, properties, body_bytes):
        logger.debug(body_bytes.decode())
        try:
            body = json.loads(body_bytes.decode())
        except Exception:
            logger.exception('could not parse body as json')
            return

        event = body['EventName']
        if event.startswith('s3:ObjectCreated'):
            try:
                create_handler(minio_client, es_client, body)
            except Exception:
                logger.exception('create handler failed')

        elif event.startswith('s3:ObjectRemoved'):
            try:
                delete_handler(minio_client, es_client, body)
            except Exception:
                logger.exception('delete handler failed')

    logger.info('Connected')
    channel.basic_consume(queue_name, on_message_callback=callback)
    channel.start_consuming()
