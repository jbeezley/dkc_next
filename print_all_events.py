import json

from minio_watcher import watch


def print_event(body, **kwargs):
    print(json.dumps(body, indent=2))


watch('print-events', global_handler=print_event)
