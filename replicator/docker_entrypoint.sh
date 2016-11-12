#!/bin/bash

# default values
if [[ -z "$MYSQL_SOURCE_PORT" ]]; then
    export MYSQL_SOURCE_PORT=3306
fi
if [[ -z "$KAFKA_PORT" ]]; then
    export KAFKA_PORT=9092
fi

cat << EOF > /wombat.properties
MYSQL_SOURCE_IDENTIFIER=${MYSQL_SOURCE_IDENTIFIER}
MYSQL_SOURCE_HOST=${MYSQL_SOURCE_HOST}
MYSQL_SOURCE_PORT=${MYSQL_SOURCE_PORT}
MYSQL_SOURCE_USER=${MYSQL_SOURCE_USER}
MYSQL_SOURCE_PASSWORD=${MYSQL_SOURCE_PASSWORD}
KAFKA_BROKER=${KAFKA_BROKER}
KAFKA_PORT=${KAFKA_PORT}
KAFKA_TOPIC_BASE=${KAFKA_TOPIC_BASE}
EOF

if [ ! -e /data/checkpoint.txt ]; then
    cat << EOF > /data/checkpoint.txt
BINLOG_FILENAME=${BINLOG_FILENAME}
BINLOG_POSITION=${BINLOG_POSITION}
EOF
fi
export CONFIG_FILE=/wombat.properties
export CHECKPOINT_FILE=/data/checkpoint.txt
exec java -jar /replicator.jar
