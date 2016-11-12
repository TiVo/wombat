#!/usr/bin/python
######################################################################
#
# File: dump_mysql.py
#
# Copyright 2015 TiVo Inc. All Rights Reserved.
#
######################################################################
"""
Takes a MySQL table, and dumps it to a file. Each line will be a json
line. It will have a "primary key" key and a value key.

Usage: dump_mysql.py <IP-address> <port> <user> <password> <table>
"""

import sys
import MySQLdb
import MySQLdb.cursors
import json
import datetime

# json.dumps() something that doesn't have a json-able
# structure. Dates, in particular, are converted to isoformat.
class StrStructEncoder(json.JSONEncoder):

    # E0202: An attribute inherited from JSONEncoder hides this method.
    # pylint: disable=E0202
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        else:
            return super(StrStructEncoder, self).default(obj)

def usage():
    print __doc__
    sys.exit(1)

def main():
    if len(sys.argv) != 6:
        usage()

    (host, port, user, password, table) = sys.argv[1:6]

    sql_db = MySQLdb.connect(
        host = host,
        port = int(port),
        user = user,
        passwd = password,
        cursorclass=MySQLdb.cursors.DictCursor)

    
    query = sql_db.cursor()

    # e.g. show index from dbName.tableName where Key_name = 'PRIMARY'
    sql = """show index from %s where Key_name = 'PRIMARY'""" % table
    query.execute(sql)
    primary_keys = query.fetchall()
    in_order = sorted(primary_keys, key=lambda x: x['Seq_in_index'])
    names = [ d['Column_name'] for d in in_order ]
    
    sql = """select * from %s""" % table
    query.execute(sql)
    results = query.fetchall()
    all = []
    for d in results:
        primary_key = dict([ (k, d[k]) for k in names])
        sorted_key_values = tuple([ d[k] for k in names ])
        dict_with_key_and_value = { 'primary_key' : primary_key,
                                    'primary_key_values' : sorted_key_values,
                                    'row' : d }
        all.append(dict_with_key_and_value)

    all = sorted(all, key=lambda d: d['primary_key_values'])
    for r in all:
        print json.dumps(r, cls=StrStructEncoder, sort_keys=True)

if __name__ == "__main__":
    main()
