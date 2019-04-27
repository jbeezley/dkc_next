from flask import Flask, jsonify, redirect
from minio_watcher import get_elasticsearch_client, get_minio_client


app = Flask(__name__)

es_client = get_elasticsearch_client()
minio_client = get_minio_client()


@app.route('/download/<hashsum>')
def download_file(hashsum):
    result = es_client.search(index='sha256', size=1, body={
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
