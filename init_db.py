#!/usr/bin/env python3
#
# This script create custom types and tables in cassandra
#
# Maintainer: glozanoa <glozanoa@uni.pe>

import argparse
from getpass import getpass

# THESE IMPORTS NEED OF cassandra-driver PYTHON PACKAGE
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

types_definition = {
    "partition": 
    """
    CREATE TYPE partition (
        name text,
        nodes text,
        state text,
        account frozen<map<text, text>>,
        resources frozen<map<text, int>>,
        restrictions frozen<map<text, text>>,
        priorities frozen<map<text, int>>
    )
    """,

    "node":
    """
    CREATE TYPE node (
        name text,
        partitions frozen<list<text>>,
        resources frozen<map<text, int>>,
        os frozen<map<text, text>>,
        status frozen<map<text, text>>,
        alloc frozen<map<text, int>>
    )
    """,

    "job":
    """
    CREATE TYPE job (
        job_id int,
        name text,
        job_state text,
        partition text,
        command text,
        array frozen<map<text, int>>,
        resources frozen<map<text, int>>,
        dependency frozen<list<int>>
    )
    """
}

tables_definition = {
    "partitions":
    """
    CREATE TABLE partitions (
        name text PRIMARY KEY,
        info partition
    )
    """,

    "nodes":
    """
    CREATE TABLE nodes (
        name text PRIMARY KEY,
        info node
    )
    """,

    "jobs":
    """
    CREATE TABLE jobs (
        job_id int PRIMARY KEY,
        info job
    )
    """
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', help='Role of cassandra DB')
    parser.add_argument('-k', '--keyspace', help='Cassandra keyspace')
    parser.add_argument('--host', help='Hostname or IP4 of cassandra DB server')

    args = parser.parse_args()

    cluster = None
    try:
        print(f"Connecting to Cassanda server (keyspace:{args.keyspace})")
        password = getpass(prompt=f"Password for {args.user} role: ")

        auth_provider = PlainTextAuthProvider(username=args.user, password=password)
        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect(args.keyspace)

        #import pdb; pdb.set_trace()

        print(f"[+] Creating user define types in {args.keyspace} keyspace")

        for type, cmd in types_definition.items():
            print(f"[*] Creating {type} type")
            session.execute(cmd)
        
        print(f"[+] Creating tables in {args.keyspace} keyspace")

        for table, cmd in tables_definition.items():
            print(f"[*] Creating {table} table")
            session.execute(cmd)

    except Exception as error:
        if cluster:
            cluster.shutdown()
            
        print(error)
        exit(1)