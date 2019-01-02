#!/bin/bash

docker-compose exec schema-registry \
    kafka-avro-console-consumer \
        --bootstrap-server kafka:29092 \
        --topic events --from-beginning
