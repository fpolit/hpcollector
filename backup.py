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
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.auth import PlainTextAuthProvider
from cassandra.util import OrderedMapSerializedKey

# importing tables
from tables import (
    Jobs,
    Partitions,
    Nodes
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', help='Role of cassandra DB')
    parser.add_argument('-k', '--keyspace', help='Cassandra keyspace')
    parser.add_argument('--hosts', default=["127.0.0.1"], nargs='+', 
                    help='Hostname or IP4 of cassandra DB server')
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
                import pdb; pdb.set_trace()
                print(f"[*] Performing a backup to {table} table")
                keyspace_data[table] = []
                rows = session.execute(f"select * from {args.keyspace}.{table}")
                for row in rows:
                    #print(f"row: {row}")
                    row2dict = {}
                    for key, value in row._asdict().items():
                        if isinstance(value, OrderedMapSerializedKey):
                            value = dict(value)      
                        row2dict[key] = value
                    keyspace_data[table].append(row2dict)

            with open(args.data, 'w') as data_file:
               json.dump(keyspace_data, data_file, indent=4)            

            print(f"[+] Backup was saved to {args.data}")

        else:

            tables = {
                "partitions": Partitions,
                "nodes": Nodes,
                "jobs": Jobs
            }

            keyspace_data = None
            with open(args.data, 'r') as data_file:
                keyspace_data = json.load(data_file)

            if keyspace_data:
                connection.setup(args.hosts, args.keyspace, protocol_version=3, auth_provider=auth_provider)

                for table_name, rows in keyspace_data.items():
                    #import pdb; pdb.set_trace()
                    print(f"[*] Inserting {len(rows)} rows in {table_name} table")

                    model = tables[table_name]
                    sync_table(model)

                    for data in rows: 
                        purged_data = model.purge_args(**data)
                        model.create(**purged_data)

                print("[+] The data of file {args.data} was successfully loaded in {args.keyspace} keyspace")

            else:
                raise Exception("Failed to read backup file")

    except Exception as error:
        if cluster:
            cluster.shutdown()

        print(error)