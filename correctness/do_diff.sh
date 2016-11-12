#!/bin/bash
#
######################################################################
#
# File: do_diff.sh
#
# Copyright 2015 TiVo Inc. All Rights Reserved.
#
######################################################################

# To have a script terminate if one of the subcommands fails
set -e
set -x

MYSQL_SOURCE_HOST=mysqlhost
MYSQL_SOURCE_PORT=10000
MYSQL_SOURCE_USER=username
MYSQL_SOURCE_PASSWORD=password

MYSQL_TABLE=dbname.tablename
MYSQL_DATABASE=dbname

KAFKA_BROKER=kafkabroker:9092
KAFKA_TOPIC=kafkatopic

DOCKER_IP=192.168.59.103

# Dump the MySQL table to a file for diffing
echo "Dumping ${MYSQL_TABLE} from ${MYSQL_SOURCE_HOST}:${MYSQL_SOURCE_PORT} for comparison" 
python correctness/dump_mysql.py ${MYSQL_SOURCE_HOST} ${MYSQL_SOURCE_PORT} ${MYSQL_SOURCE_USER} ${MYSQL_SOURCE_PASSWORD} ${MYSQL_TABLE} > source.json

# Capture the MySQL table schema, so we can create a temporary MySQL instance with the same schema
mysqldump --host=${MYSQL_SOURCE_HOST} --port=${MYSQL_SOURCE_PORT} --user=${MYSQL_SOURCE_USER} --password=${MYSQL_SOURCE_PASSWORD}  --no-data --database ${MYSQL_DATABASE}  > create_table_schema.sql

# Create temporary MySQL instance
echo "Launching temporary MySQL instance via docker..."
docker run --name restored -e MYSQL_ROOT_PASSWORD=temporary -p 9000:3306 -d mariadb:5.5

# Wait for it to start up
sleep 20
echo "Done launching."

# Create empty tables on temporary MySQL instance
mysql --protocol=tcp --host=${DOCKER_IP} --port=9000  --user=root --password=temporary < create_table_schema.sql

# Load kafka topic into temporary MySQL instance
# The loading file needs to know something about the MySQL schema, so go look there for any custom edits required.
time python correctness/kafka_to_mysql.py ${KAFKA_TOPIC} ${KAFKA_BROKER} ${DOCKER_IP} 9000 root temporary ${MYSQL_TABLE}

# Dump temporary MySQL instance to a file for diffing
echo "Dumping ${MYSQL_TABLE} from temporary MySQL instance for comparison"
python correctness/dump_mysql.py ${DOCKER_IP} 9000 root temporary ${MYSQL_TABLE}  > restored.json

# Diff into buckets: only in source, only in kafka, same in both, different
# Ideally, everything should be in "same".
echo "Calculating differences..."
python correctness/diff_to_buckets.py source.json  restored.json db-only.json kafka-only.json same.json different.json

echo "Done. See the following files: db-only.json kafka-only.json same.json different.json"
# Also do a standard text diff
diff source.json restored.json

# Clean up the temporary instance
#docker rm -f restored


