#!/usr/bin/env bash

docker-compose up -d --scale app=0 --scale driver=0 --scale reader=0