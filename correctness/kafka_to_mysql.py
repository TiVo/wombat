#!/usr/bin/python
######################################################################
#
# File: kafka_to_mysql.py
#
# Copyright 2015 TiVo Inc. All Rights Reserved.
#
######################################################################
"""
Usage: kafka_to_mysql.py <kafka_topic> <kafka_broker> <mysql-ip> <mysql-port> <mysql-user> <mysql-password> <mysql_table>
"""

import json
import MySQLdb
from kafka import KafkaClient, KafkaConsumer
import sys

def usage():
    print __doc__
    sys.exit(1)

def main():
    # R0915: "too many statements in function (>50)"
    # pylint: disable=R0915

    if len(sys.argv) != 8:
        print "Wrong number of arguments"
        usage()

    (kafka_topic, kafka_broker, mysql_host, mysql_port, mysql_user, mysql_password, mysql_table) = sys.argv[1:8]

    sql_db = MySQLdb.connect(
        host = mysql_host,
        port = int(mysql_port),
        user = mysql_user,
        passwd = mysql_password)

    query = sql_db.cursor()

    client = KafkaClient(kafka_broker)
    


    consumer = KafkaConsumer(kafka_topic, metadata_broker_list = [kafka_broker],
                             auto_commit_enable = False,
                             auto_offset_reset='smallest')

    last_offsets = {}
    partition_ids = client.get_partition_ids_for_topic(kafka_topic)
    for partition in partition_ids:
        offsets = consumer.get_partition_offsets(kafka_topic, partition, -1, 1)
        print offsets
        
        # Don't really understand this format, so put in asserts
        # (Pdb) consumer.get_partition_offsets("topicname", 0, -1, 1)
        # (15471)
        assert len(offsets) == 1
        assert offsets[0] > 0

        next_offset = offsets[0]
        last_offset = next_offset - 1
        last_offsets[partition] = last_offset

    finished_partitions = set()

    print last_offsets
    count = 0

    # mapping from primary key tuples, to row data
    insert_batch = {}
    insert_sql = None

    for m in consumer:
        if m.partition in finished_partitions:
            continue

        count += 1

        payload = m.value
        (first_line, rest) = payload.split("\r\n", 1)
        (_notused, header_len, _body_len) = first_line.split(" ")
        header_len = int(header_len)
        body = rest[header_len:]

        primary_key_str = m.key
        #            import pdb; pdb.set_trace()
        primary_keys = json.loads(primary_key_str)
        primary_tuples = sorted(primary_keys.items())
        sorted_primary_key_names = [ k for (k,v) in primary_tuples ]
        sorted_primary_key_values = [ int(v) for (k,v) in primary_tuples ]

        if len(body) > 0:
            # This is a write
            data = json.loads(body)
                
            # date fields have to be turned from a number back into a datetime object
            date_fields = ['createDate', 'updateDate']
            for d in date_fields:
                if d not in data:
                    continue
                val = data[d]
                if val is None:
                    continue
                if val == -62170156800000:
                    # this is hacky and a sign that i'm doing something wrong, I think.
                    val = "0000-00-00 00:00:00"
                else:
                    val = val/1000
                    import datetime; 
                    val = datetime.datetime.utcfromtimestamp(val)
                data[d] = val

            keys = [ k for (k, v) in sorted(data.items()) ]
            values = [ v for (k, v) in sorted(data.items()) ]

            keys_wo_primary = [ k for (k, v) in sorted(data.items()) ]
            for p in sorted_primary_key_names:
                keys_wo_primary.remove(p)

            # e.g.
            # insert into dbname.tablename (col1, col2) values (%s, %s) on duplicate key update col2 = values(col2)
            # assuming that col1 is the primary key
            insert_sql = """insert into %s """ % mysql_table
            insert_sql += """ (%s) """ % (", ".join(keys))
            insert_sql += " values (%s) " % (", ".join(["%s"] * len(values) ))
            insert_sql +=  "on duplicate key update "
            insert_sql += ", ".join(["%s = values(%s)" % (k, k) for k in keys_wo_primary ])
            insert_batch[tuple(primary_tuples)] = tuple(values)
            if len(insert_batch) > 5000:
                query.executemany(insert_sql, insert_batch.values())
                sql_db.commit()
                insert_batch = {}
        else:
            # This is a delete
            if len(insert_batch) > 0 and insert_sql is not None:
                # flush all writes before processing any deletes
                query.executemany(insert_sql, insert_batch.values())
                sql_db.commit()
                insert_batch = {}

            # get the primary keys, and delete the row
            where_clause = ' and '.join([ "%s = %%s" % k for k in sorted_primary_key_names ])
            # e.g.
            # delete from dbname.tablename where field1 = %s and field2 = %s
            delete_sql = """delete from %s where %s""" % (mysql_table, where_clause)
            values = tuple(sorted_primary_key_values)
            query.execute(delete_sql, values)
            sql_db.commit()

        # how do I know when to stop?
        print "Partition %d Offset %d of %d" % (m.partition, m.offset, last_offsets.get(m.partition))
        if m.offset >= last_offsets.get(m.partition):
            finished_partitions.add(m.partition)
            if len(finished_partitions) == len(last_offsets):
                # All partitions are done.
                break

    if len(insert_batch) > 0:
        # flush any remaining writes
        query.executemany(insert_sql, insert_batch.values())
        sql_db.commit()
        insert_batch = {}

    print "Imported %d messages into mysql" % count

if __name__ == "__main__":
    main()
