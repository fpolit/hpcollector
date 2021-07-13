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
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.auth import PlainTextAuthProvider

# importing tables
from tables import (
    Jobs,
    Partitions,
    Nodes
)


logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')

try:
    import pyslurm
except ModuleNotFoundError as error:
    logging.critical("No PySlurm package installed")
    exit(1)


def nodes_collector(keyspace:str, n:int = 10, verbose:bool = False):
    """
    Collect information of nodes of a cluster with a frequency n
    """
    import pdb; pdb.set_trace()
    nodes = pyslurm.node()

    sync_table(Nodes, [keyspace])
    nodes_ids = set(node.name for node in Nodes.objects.all())

    time.sleep(2)

    while True:
        update_nodes_ids = set(nodes.ids())
        
        if verbose:
            logging.info("Checking for new nodes")
        
        #new nodes was found
        for node_id in update_nodes_ids - nodes_ids:
            try:
                node_data  = nodes.find_id(node_id)

            except Exception as error:
                logging.error(error)
                logging.info(f"Unable to get information of node {node_id}")
                continue

            purged_data = Nodes.purge_args(**node_data)
            logging.info(f"New node was found {node_id}")
            logging.info(f"Collecting data of node {node_id}")
            new_node = Nodes.create(**purged_data)

            if verbose:
                logging.info(f"Node data: {purged_data}")


        if verbose:
            logging.info("Checking if any node was updated")

        # check if a node was changed
        for node_id in nodes_ids:
            try:
                node_data  = nodes.find_id(node_id)

            except Exception as error:
                logging.error(error)
                logging.info(f"Unable to get information of node {node_id}")
                continue
            
            purged_data = Nodes.purge_args(**node_data)
            old_node_model = Nodes.objects.filter(name = node_id)
            old_node = old_node_model.get()
            updated_node = Nodes(**purged_data)


            if old_node != updated_node: #check if data of old job was changed
                #import pdb; pdb.set_trace()
                updated_cols = Nodes.updated_columns(old_node, updated_node)
                logging.info(f"Node {node_id} was updated")
                logging.info(f"Updating data of node {node_id}")
                
                old_node_model.update(**updated_cols)


        if verbose:
            logging.info(f"Defined nodes: {update_nodes_ids}")

        nodes_ids = update_nodes_ids
        time.sleep(n)

    # except KeyboardInterrupt:
    #     logging.info("Stop collecting information of nodes.")


def partitions_collector(keyspace:str, n:int = 10, verbose:bool = False):
    """
    Collect information of partitions of a cluster with a frequency n
    """
    #import pdb; pdb.set_trace()
    partitions = pyslurm.partition()

    sync_table(Partitions, [keyspace])
    partitions_ids = set(partition.name for partition in Partitions.objects.all())

    time.sleep(5)
    #import pdb; pdb.set_trace()
    while True:
        update_partitions_ids = set(partitions.ids())
        
        if verbose:
            logging.info("Checking for new partitions")

        #new partition was created
        for partition_id in update_partitions_ids - partitions_ids:
            try:
                partition_data  = partitions.find_id(partition_id)
            except Exception as error:
                logging.error(error)
                logging.info(f"Unable to get information of partition {partition_id}")
            
            purged_data = Partitions.purge_args(**partition_data)
            logging.info(f"New partition was found {partitions_id}")
            logging.info(f"Collecting data of partition {partition_id}")
            new_partition = Partitions.create(**purged_data)
            
            if verbose:
                logging.info(f"Partition data: {purged_data}")


        if verbose:
            logging.info("Checking if any partition was updated")

        # check if a partition was changed
        for partition_id in partitions_ids:
            try:
                partition_data  = partitions.find_id(partition_id)
            except Exception as error:
                logging.error(error)
                logging.info(f"Unable to get information of partition {partition_id}")

            purged_data = Partitions.purge_args(**partition_data)
            old_partition_model = Partitions.objects.filter(name = partition_id)
            old_partition = old_partition_model.get()
            updated_partition = Partitions(**purged_data)


            if old_partition != updated_partition: #check if data of old job was changed
                #import pdb; pdb.set_trace()
                updated_cols = Jobs.updated_columns(old_job, updated_job)
                logging.info(f"Partition {partition_id} was updated")
                logging.info(f"Updating data of partition {partition_id}")
                
                old_partition_model.update(**updated_cols)

        if verbose:
            logging.info(f"Defined partitions: {update_partitions_ids}")

        partitions_ids = update_partitions_ids
        time.sleep(n)

    # except KeyboardInterrupt:
    #     logging.info("Stop collecting information of partitions.")


def jobs_collector(keyspace:str, n:int = 1, verbose:bool = False):
    """
    Collect information of jobs submitted in a cluster with a frequency n
    """
    #import pdb; pdb.set_trace()
    sync_table(Jobs, [keyspace])

    jobs = pyslurm.job()

    job_ids = set(job.job_id for job in Jobs.objects.all())
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

            purged_data = Jobs.purge_args(**job_data)
            logging.info(f"New job was summited: {new_job_id}")
            logging.info(f"Collecting data of job {new_job_id}")
            new_job = Jobs.create(**purged_data)

            if verbose:
                logging.info(f"Job data: {purged_data}")

            

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

            purged_data = Jobs.purge_args(**job_data)
            old_job_model = Jobs.objects.filter(job_id = job_id)
            old_job = old_job_model.get()
            updated_job = Jobs(**purged_data)


            if old_job != updated_job: #check if data of old job was changed
                #import pdb; pdb.set_trace()
                updated_cols = Jobs.updated_columns(old_job, updated_job)
                logging.info(f"Job {job_id} was updated")
                logging.info(f"Updating data of job {job_id}")
                
                old_job_model.update(**updated_cols)

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
    parser.add_argument('--hosts', default=["127.0.0.1"], nargs='+', 
                        help='Hostname or IP4 of cassandra DB server')
    parser.add_argument('-v', '--verbose', action='store_true', 
                        help='Show collected data')
    parser.add_argument('-f', '--freq', nargs=3, default=[10, 10, 1],
                        help='Collection frequency (NODES, PARTITIONS, JOBS)')

    args = parser.parse_args()

    auth_provider = None
    try:
        print(f"Connecting to Cassanda server (keyspace:{args.keyspace})")
        password = getpass(prompt=f"Password for {args.user} role: ")

        auth_provider = PlainTextAuthProvider(username=args.user, password=password)
        del password

        connection.setup(args.hosts, args.keyspace, protocol_version=3, auth_provider=auth_provider)
        # cluster = Cluster(auth_provider=auth_provider)
        # session = cluster.connect(args.keyspace)
    except Exception as error:
        if auth_provider:
            del auth_provider
        print(error)
        exit(1)

    collector = []
    try:
        nodes_collector(args.keyspace, 1, args.verbose)
        # collector_func = [nodes_collector, partitions_collector, jobs_collector]
        # collector_args = [(cluster, args.keyspace, fc, args.verbose) for fc in args.freq]

        # for func, args in zip(collector_func, collector_args):
        #     collector.append(threading.Thread(target=func, args=args, daemon=True))

        # logging.info("Start collecting information")
        # for thread_collector in collector:
        #     thread_collector.start()

        # try:
        #     while True:
        #         time.sleep(1)
        # except KeyboardInterrupt:
        #     logging.info("Stop collecting information.")
        #     exit(0)


    except Exception as error:
        logging.error(error)