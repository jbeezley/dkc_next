#!/bin/bash

# These lines need to be run on first run to configure minio.
mc mb local/test
mc admin config set local < config.json
mc admin service restart
mc event add local/test arn:minio:sqs::1:amqp --event put,delete
