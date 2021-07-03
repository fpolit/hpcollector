#!/usr/bin/env python3
#
# Collect data of nodes, partitions and job of a cluster
#
# Maintainer: glozanoa <glozanoa@uni.pe>

import argparse
from typing import Any, List
import threading
import time
import logging
from getpass import getpass

# THESE IMPORTS NEED OF cassandra-driver PYTHON PACKAGE
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# importing UDTs (user defined types)
from node import Node
from partition import Partition
from job import Job

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')

try:
    import pyslurm
except ModuleNotFoundError as error:
    logging.critical("No PySlurm package installed")
    exit(1)


def nodes_collector(cluster:Cluster, keyspace:str, n:int = 10, verbose:bool = False):
    """
    Collect information of nodes of a cluster with a frequency n
    """
    try:
        nodes = pyslurm.node()

        session = cluster.connect(keyspace)
        cluster.register_user_type(keyspace, 'node', Node)
        
        insert_statement = session.prepare(f"INSERT INTO {keyspace}.nodes (name, info) VALUES (?, ?)")
        update_statement = session.prepare(f"UPDATE {keyspace}.nodes SET info=? WHERE name=?")

        nodes_ids = set(row.name for row in session.execute(f"SELECT name FROM {keyspace}.nodes"))
        time.sleep(2)
        #import pdb; pdb.set_trace()

        while True:
            update_nodes_ids = set(nodes.ids())
            
            if verbose:
                logging.info("Checking for new nodes")
            
            #new nodes was found
            for node_id in update_nodes_ids - nodes_ids:
                try:
                    node_details  = nodes.find_id(node_id)

                except Exception as error:
                    logging.error(error)
                    logging.info(f"Unable to get information of node {node_id}")
                    continue

                node = Node(**node_details)
                logging.info(f"New node was found: {node}")
                logging.info(f"Collecting data of node {node_id}")
                session.execute(insert_statement, [node_id, node])

            
            if verbose:
                logging.info("Checking if any node was updated")

            # check if a node was changed
            for node_id in nodes_ids:
                try:
                    node_details  = nodes.find_id(node_id)

                except Exception as error:
                    logging.error(error)
                    logging.info(f"Unable to get information of node {node_id}")
                    continue
                
                if Node.was_changed(node_id, node_details, session, keyspace):
                    logging.info(f"Configuration of node {node_id} was changed")
                    logging.info(f"Updating data of node {node_id}")
                    updated_node = Node(**node_details)
                    session.execute(update_statement, [updated_node, node_id])


            if verbose:
                logging.info(f"Defined nodes: {update_nodes_ids}")

            nodes_ids = update_nodes_ids
            time.sleep(n)

    except KeyboardInterrupt:
        logging.info("Stop collecting information of nodes.")


def partitions_collector(cluster:Cluster, keyspace:str, n:int = 10, verbose:bool = False):
    """
    Collect information of partitions of a cluster with a frequency n
    """
    try:
        #import pdb; pdb.set_trace()
        partitions = pyslurm.partition()

        session = cluster.connect(keyspace)
        cluster.register_user_type(keyspace, 'partition', Partition)
        
        insert_statement = session.prepare(f"INSERT INTO {keyspace}.partitions (name, info) VALUES (?, ?)")
        update_statement = session.prepare(f"UPDATE {keyspace}.partitions SET info=? WHERE name=?")

        partitions_ids = set(row.name for row in session.execute(f"SELECT name FROM {keyspace}.partitions"))
        time.sleep(5)
        #import pdb; pdb.set_trace()
        while True:
            update_partitions_ids = set(partitions.ids())
            
            if verbose:
                logging.info("Checking for new partitions")

            #new partition was created
            for partition_id in update_partitions_ids - partitions_ids:
                try:
                    partition_details  = partitions.find_id(partition_id)
                except Exception as error:
                    logging.error(error)
                    logging.info(f"Unable to get information of partition {partition_id}")

                partition = Partition(**partition_details)
                logging.info(f"New partition was found: {partition}")
                logging.info(f"Collecting data of partition {partition_id}")
                session.execute(insert_statement, [partition_id, partition])
        
            if verbose:
                logging.info("Checking if any partition was updated")

            # check if a partition was changed
            for partition_id in partitions_ids:
                try:
                    partition_details  = partitions.find_id(partition_id)
                except Exception as error:
                    logging.error(error)
                    logging.info(f"Unable to get information of partition {partition_id}")

                if Partition.was_changed(partition_id, partition_details, session, keyspace):
                    #import pdb; pdb.set_trace()
                    logging.info(f"Configuration of partition {partition_id} was changed")
                    logging.info(f"Updating data of partition {partition_id}")
                    updated_partition = Partition(**partition_details)
                    session.execute(update_statement, [updated_partition, partition_id])

            if verbose:
                logging.info(f"Defined partitions: {update_partitions_ids}")

            partitions_ids = update_partitions_ids
            time.sleep(n)

    except KeyboardInterrupt:
        logging.info("Stop collecting information of partitions.")


def jobs_collector(cluster:Cluster, keyspace:str, n:int = 1, verbose:bool = False):
    """
    Collect information of jobs submitted in a cluster with a frequency n
    """
    #import pdb; pdb.set_trace()
    #try:

    session = cluster.connect(keyspace)
    cluster.register_user_type(keyspace, 'job', Job)
    
    insert_statement = session.prepare(f"INSERT INTO {keyspace}.jobs (job_id, info) VALUES (?, ?)")
    update_statement = session.prepare(f"UPDATE {keyspace}.jobs SET info=? WHERE job_id=?")

    jobs = pyslurm.job()

    job_ids = set(row.job_id for row in session.execute(f"SELECT job_id FROM {keyspace}.jobs"))
    #import pdb; pdb.set_trace()
    while True:
        updated_job_ids = set(jobs.ids())
        
        if verbose:
            logging.info("Checking for new submitted jobs")

        # new jobs was submmited
        for new_job_id in updated_job_ids - job_ids:
            try:
                job_data = jobs.find_id(new_job_id)[0]

            except Exception as error:
                logging.error(error)
                logging.info(f"Unable to get information of job {new_job_id}")
                continue

            new_job = Job(**job_data)
            logging.info(f"New job was summited: {new_job}")
            logging.info(f"Collecting data of job {new_job_id}")
            #import pdb; pdb.set_trace()
            session.execute(insert_statement, [new_job_id, new_job])

        if verbose:
            logging.info("Checking if any job was updated")

        # check if data of old job was changed
        for job_id in job_ids:
            try:
                job_data = jobs.find_id(job_id)[0]

            except Exception as error:
                logging.error(error)
                logging.info(f"Unable to get information of job {job_id}")
                continue

            if Job.was_changed(job_id, job_data, session, keyspace): #check if data of old job was changed
                #import pdb; pdb.set_trace()
                logging.info(f"Job {job_id} was updated")
                logging.info(f"Updating data of job {job_id}")
                updated_job = Job(**job_data)
                session.execute(update_statement, [updated_job, job_id])

        if verbose:
            logging.info(f"Submitted jobs: {updated_job_ids}")

        job_ids = updated_job_ids
        time.sleep(n)

    # except Exception as error:
    #     logging.error(error)
    #     logging.info("Stop collecting information of jobs.")


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', required=True, help='Role of cassandra DB')
    parser.add_argument('-k', '--keyspace', required=True, help='Cassandra keyspace')
    parser.add_argument('--host', default="127.0.0.1", help='Hostname or IP4 of cassandra DB server')
    parser.add_argument('-v', '--verbose', action='store_true', 
                        help='Show collected data')
    parser.add_argument('-f', '--freq', nargs=3, default=[10, 10, 1],
                        help='Collection frequency (NODES, PARTITIONS, JOBS)')

    args = parser.parse_args()

    auth_provider = None
    cluster = None
    try:
        print(f"Connecting to Cassanda server (keyspace:{args.keyspace})")
        password = getpass(prompt=f"Password for {args.user} role: ")

        auth_provider = PlainTextAuthProvider(username=args.user, password=password)
        del password

        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect(args.keyspace)
    except Exception as error:
        if auth_provider:
            del auth_provider
        if cluster:
           cluster.shutdown()
        exit(1)

    collector = []
    try:
        #jobs_collector(cluster, args.keyspace, 1, args.verbose)
        collector_func = [nodes_collector, partitions_collector, jobs_collector]
        collector_args = [(cluster, args.keyspace, fc, args.verbose) for fc in args.freq]

        for func, args in zip(collector_func, collector_args):
            collector.append(threading.Thread(target=func, args=args))

        logging.info("Start collecting information")
        for thread_collector in collector:
            thread_collector.start()

    except KeyboardInterrupt as error:
        #import pdb; pdb.set_trace()
        if collector:
            for thread_collector in collector:
                thread_collector.join()
    finally:
        logging.info("Stop collecting information.")
        if cluster:
            cluster.shutdown()