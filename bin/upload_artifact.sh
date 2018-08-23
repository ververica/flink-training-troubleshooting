#!/usr/bin/env bash

HOST=$1

${0%/*}/mc config host add minio http://$HOST:30001 admin password
${0%/*}/mc cp ${0%/*}/../target/flink-training-troubleshooting-0.1.jar minio/flink