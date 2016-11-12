#!/bin/bash

# expects that kafka, zookeeper, and mariadb are all linked properly
cat << EOF > /wombat.properties
MYSQL_SOURCE_IDENTIFIER=serverIdentifier
MYSQL_SOURCE_HOST=${MARIADB_PORT_3306_TCP_ADDR}
MYSQL_SOURCE_PORT=${MARIADB_PORT_3306_TCP_PORT}
MYSQL_SOURCE_USER=root
MYSQL_SOURCE_PASSWORD=${MARIADB_ENV_MYSQL_ROOT_PASSWORD}
KAFKA_BROKER=${KAFKA_PORT_9092_TCP_ADDR}
KAFKA_PORT=${KAFKA_PORT_9092_TCP_PORT}
KAFKA_TOPIC_BASE=prefix
EOF

if [ ! -e /data/checkpoint.txt ]; then
    cat << EOF > /data/checkpoint.txt
BINLOG_FILENAME=mariadb-bin.000008
BINLOG_POSITION=1254
EOF
fi
export CONFIG_FILE=/wombat.properties
export CHECKPOINT_FILE=/data/checkpoint.txt
exec java -jar /replicator.jar
