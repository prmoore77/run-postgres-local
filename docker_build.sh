#!/bin/bash

set -e

docker build . --tag postgres_with_parquet_fdw
