import os

import dotenv
from elasticsearch import Elasticsearch
from flask import Flask, jsonify, redirect
from minio import Minio


dotenv.load_dotenv(os.getenv('DOTENV_PATH'))
app = Flask(__name__)

es_client = Elasticsearch([os.getenv('ELASTICSEARCH_HOST')])

minio_host = os.getenv('MINIO_HOST', 'localhost')
minio_port = os.getenv('MINIO_PORT', 9000)
minio_client = Minio(
    f'{minio_host}:{minio_port}',
    access_key=os.getenv('MINIO_ACCESS_KEY'),
    secret_key=os.getenv('MINIO_SECRET_KEY'),
    secure=False
)


@app.route('/download/<hashsum>')
def download_file(hashsum):
    result = es_client.search(index='files', size=1, body={
        'query': {
            'match': {
                'sha256': hashsum
            }
        }
    })
    hits = result['hits']['hits']
    if not hits:
        return jsonify('File not found'), 404

    bucket, file = hits[0]['_id'].split('/', maxsplit=1)

    return redirect(minio_client.presigned_url('GET', bucket, file))
