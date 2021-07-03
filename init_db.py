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
    CREATE TYPE IF NOT EXISTS partition (
        name text,
        state text,
        nodes text,
        allow_accounts  text,
        allow_alloc_nodes   text,
        allow_groups    text,
        allow_qos   text,
        cr_type int,
        def_mem_per_node    text,
        default_time    text,
        default_time_str    text,
        grace_time  int,
        max_cpus_per_node text,
        max_mem_per_node    text,
        max_nodes   text,
        max_share   int,
        max_time    text,
        max_time_str    text,
        min_nodes   int,
        over_time_limit text,
        preempt_mode    text,
        priority_job_factor int,
        priority_tier   int,
        total_cpus  int,
        total_nodes int,
        tres_fmt_str    text
    )
    """,

    "node":
    """
    CREATE TYPE IF NOT EXISTS node (
        name text,
        state text,
        partitions list<text>,
        arch    text,
        boards  int,
        boot_time int,
        cores int,
        core_spec_cnt   int,
        cores_per_socket    int,
        cpus    int,
        cpu_load    int,
        cpu_spec_list   list<text>,
        features    list<text>,
        features_active list<text>,
        free_mem    int,
        gres    list<text>,
        gres_drain text,
        mem_spec_limit int,
        node_addr text,
        node_hostname text,
        os text,
        real_memory int,
        slurmd_start_time   int,
        sockets int,
        threads int,
        tmp_disk    int,
        weight  int,
        tres_fmt_str    text,
        version text,
        reason_uid  int,
        power_mgmt  map<text, int>,
        energy  map<text, int>,
        alloc_cpus  int,
        err_cpus    int,
        alloc_mem   int
    )
    """,

    "job":
    """
    CREATE TYPE IF NOT EXISTS job (
        job_id  int,
        name    text,
        job_state   text,
        partition   text,
        command text,
        account text,
        accrue_time text,
        admin_comment   text,
        alloc_node  text,
        alloc_sid   int,
        assoc_id    int,
        batch_flag  int,
        billable_tres   float,
        bitflags    bigint,
        boards_per_node int,
        comment text,
        contiguous  boolean,
        cores_per_socket    int,
        cpus_per_task   int,
        derived_ec  text,
        eligible_time   bigint,
        end_time    bigint,
        exc_nodes   list<text>,
        exit_code   text,
        features    list<text>,
        group_id    int,
        last_sched_eval	    text,
        licenses    map<text, text>,
        max_cpus    int,
        max_nodes   int,
        nodes   text,
        nice    int,
        ntasks_per_core int,
        ntasks_per_core_str text,
        ntasks_per_node int,
        ntasks_per_socket   int,
        ntasks_per_socket_str   text,
        ntasks_per_board    int,
        num_cpus    int,
        num_nodes   int,
        num_tasks   int,
        mem_per_cpu boolean,
        min_memory_cpu  text,
        mem_per_node    boolean,
        min_memory_node int,
        pn_min_memory   int,
        pn_min_cpus int,
        pn_min_tmp_disk int,
        power_flags int,
        priority    bigint,
        profile int,
        reboot  int,
        req_nodes   list<text>,
        req_switch  int,
        requeue boolean,
        resize_time int,
        restart_cnt int,
        run_time    int,
        run_time_str    text,
        shared  text,
        show_flags  int,
        sockets_per_board   int,
        sockets_per_node    int,
        start_time  bigint,
        state_reason    text,
        std_err text,
        std_in  text,
        std_out text,
        submit_time bigint,
        suspend_time    int,
        system_comment  text,
        time_limit  text,
        time_min    int,
        tres_req_str    text,
        user_id int,
        wait4switch int,
        work_dir    text,
        cpus_allocated  map<text, int>
    )
    """
}

tables_definition = {
    "partitions":
    """
    CREATE TABLE IF NOT EXISTS partitions (
        name text PRIMARY KEY,
        info partition
    )
    """,

    "nodes":
    """
    CREATE TABLE IF NOT EXISTS nodes (
        name text PRIMARY KEY,
        info frozen<node>
    )
    """,

    "jobs":
    """
    CREATE TABLE IF NOT EXISTS jobs (
        job_id int PRIMARY KEY,
        info frozen<job>
    )
    """
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', help='Role of cassandra DB')
    parser.add_argument('-k', '--keyspace', help='Cassandra keyspace')
    parser.add_argument('--host', help='Hostname or IP4 of cassandra DB server')

    parser.add_argument('--only-types', dest="only_types", action="store_true", 
                        help='only create define types')
    parser.add_argument('--only-tables', dest="only_tables", action="store_true", 
                        help='only create define tables')

    args = parser.parse_args()

    cluster = None
    try:
        print(f"Connecting to Cassanda server (keyspace:{args.keyspace})")
        password = getpass(prompt=f"Password for {args.user} role: ")

        auth_provider = PlainTextAuthProvider(username=args.user, password=password)
        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect(args.keyspace)

        #import pdb; pdb.set_trace()

        if not args.only_tables:
            print(f"[+] Creating user define types in {args.keyspace} keyspace")

            for name, cmd in types_definition.items():
                print(f"[*] Creating {name} type")
                session.execute(cmd)
        
        if not args.only_types:
            print(f"[+] Creating tables in {args.keyspace} keyspace")

            for table, cmd in tables_definition.items():
                print(f"[*] Creating {table} table")
                session.execute(cmd)

    except Exception as error:
        if cluster:
            cluster.shutdown()
            
        print(error)
        exit(1)