#!/bin/bash

# These lines need to be run on first run to configure minio.
mc mb local/test
mc admin config set local < config.json
mc admin service restart local

# send all put/delete events to rabbitmq
mc event add local/test arn:minio:sqs::1:amqp --event put,delete

# send all events with a specific prefix to a webhook for demo purposes
mc event add local/test arn:minio:sqs::1:webhook --prefix webhook-test/
