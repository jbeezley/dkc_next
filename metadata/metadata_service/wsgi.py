from flask import Flask, jsonify
from minio_watcher import get_elasticsearch_client
from webargs import fields
from webargs.flaskparser import use_args


app = Flask(__name__)

es_client = get_elasticsearch_client()


@app.route('/search')
@use_args({
    'filter': fields.Str(),
    'limit': fields.Int(missing=10),
    'offset': fields.Int(missing=0)
})
def search(args):
    filter_query = []
    if 'filter' in args:
        key, value = args['filter'].split('=', 1)
        filter_query = [{'match': {key: value}}]

    resp = es_client.search('metadata', {
        'query': {
            'bool': {
                'must': filter_query
            }
        },
        'size': args['limit'],
        'from': args['offset']
    }, track_total_hits=True)
    total = resp['hits']['total']['value']
    names = [hit['_id'] for hit in resp['hits']['hits']]
    return jsonify({'total': total, 'files': names})
