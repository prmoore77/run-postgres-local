#!/bin/bash

# Note - you must build a local docker image first by running: ./docker_build.sh

set -e

DATA_DIR=$(greadlink --canonicalize $(dirname ${0}))/data

docker run --rm \
	--detach \
	--name local-postgres \
	--env POSTGRES_PASSWORD=mysecretpassword \
	--env PGDATA=/var/lib/postgresql/data/pgdata \
	--volume ${DATA_DIR}:/var/lib/postgresql/data \
	--publish 5432:5432 \
	postgres_with_parquet_fdw
