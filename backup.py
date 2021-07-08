#!/usr/bin/env python3
#
# Perform a backup of all the tables in a keyspace in a json file
# Also this script load data in json script to DB
#
# Maintainer: glozanoa <glozanoa@uni.pe>

import argparse
from getpass import getpass
import json


# THESE IMPORTS NEED OF cassandra-driver PYTHON PACKAGE
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import OrderedMapSerializedKey


# importing UDTs (user defined types)
from node import Node
from partition import Partition
from job import Job

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', help='Role of cassandra DB')
    parser.add_argument('-k', '--keyspace', help='Cassandra keyspace')
    parser.add_argument('--host', help='Hostname or IP4 of cassandra DB server')
    parser.add_argument('--load', action='store_true', help="Load data of json file to keyspace's tables")
    parser.add_argument('data', help="Json file to backup keyspace or to load data to keyspace's tables")

    args = parser.parse_args()

    cluster = None
    try:
        print(f"Connecting to Cassanda server (keyspace:{args.keyspace})")
        password = getpass(prompt=f"Password for {args.user} role: ")

        auth_provider = PlainTextAuthProvider(username=args.user, password=password)
        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect(args.keyspace)

        if not args.load:
            tables = [table.name for table in session.execute("DESCRIBE TABLES")]

            keyspace_data = {}
            for table in tables:
                #import pdb; pdb.set_trace()
                print(f"[*] Performing a backup to {table} table")
                keyspace_data[table] = {}
                rows = session.execute(f"select * from {args.keyspace}.{table}")
                for key, info in rows:
                    info_dict = dict(info._asdict())
                    for info_key ,value in info_dict.items():
                        if isinstance(value, OrderedMapSerializedKey):
                            info_dict[info_key] = dict(value)
                    
                    keyspace_data[table][key] = info_dict


            with open(args.data, 'w') as data_file:
                json.dump(keyspace_data, data_file, indent=4)            

            print(f"[+] Backup was saved to {args.data}")

        else:

            tables_structs = {
                "partitions": {
                    'columns': ['name', 'info'],
                    'udt': Partition,
                    'pk_type': str,
                },
                "nodes": {
                    'columns': ['name', 'info'],
                    'udt': Node,
                    'pk_type': str,
                },
                "jobs": {
                    'columns': ['job_id', 'info'],
                    'udt': Job,
                    'pk_type': int,
                }
            }

            keyspace_data = None
            with open(args.data, 'r') as data_file:
                keyspace_data = json.load(data_file)

            if keyspace_data:
                for table_name, rows in keyspace_data.items():
                    #import pdb; pdb.set_trace()
                    print(f"[*] Inserting {len(rows)} rows in {table_name} table")

                    pk, info = tables_structs[table_name]['columns']
                    pk_type = tables_structs[table_name]['pk_type']
                    udt = tables_structs[table_name]['udt']
                    cluster.register_user_type(args.keyspace, udt.__name__.lower(), udt)
                    insert_statement = session.prepare(f"INSERT INTO {args.keyspace}.{table_name} ({pk}, {info}) VALUES (?, ?)")

                    for pk, info in rows.items(): # pk: Primary key
                        session.execute(insert_statement, [pk_type(pk), udt(**info)])

            else:
                raise Exception("Failed to read backup file")

    except Exception as error:
        if cluster:
            cluster.shutdown()

        print(error)