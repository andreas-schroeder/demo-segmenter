#!/usr/bin/env bash

sbt ";app/docker:publishLocal; driver/docker:publishLocal; reader/docker:publishLocal"