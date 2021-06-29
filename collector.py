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
from cassandra.cluster import Cluster


from node import Node
from partition import Partition
from job import Job

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')

try:
    import pyslurm
except ModuleNotFoundError as error:
    logging.critical("No PySlurm package installed")
    exit(1)


def nodes_collector(n:int = 10, verbose:bool = False):
    """
    Collect information of nodes of a cluster with a frequency n
    """
    try:
        nodes = pyslurm.node()
        nodes_ids = set()
        time.sleep(2)
        while True:
            update_nodes_ids = set(nodes.ids())
            
            #new nodes was found
            for node_id in update_nodes_ids - nodes_ids:
                node_details = nodes.find_id(node_id)
                node = Node(**node_details)
                logging.info(f"New node was found: {node}")
                logging.info(f"Collecting data of node {node_id}")
                if verbose:
                    attributes = node.__dict__
                    for attr, value in attributes.items():
                            if isinstance(value, dict):
                                print(f"\t{attr}:")
                                for subattr, subvalue in value.items():
                                    print(f"\t\t{subattr}: {subvalue}")
                            else:
                                print(f"\t{attr}: {value}")
            
            # check if a node was changed
            for node_id in nodes_ids:
                node_details  = nodes.find_id(node_id)
                if False: #node data was modified (Compare attributes with node save in DB)
                    logging.info(f"Configuration of node {node_id} was changed")
                    logging.info(f"Updating data of node {node_id}")
                    if verbose:
                        attributes = node.__dict__
                        for attr, value in attributes.items():
                            if isinstance(value, dict):
                                print(f"\t{attr}:")
                                for subattr, subvalue in value.items():
                                    print(f"\t\t{subattr}: {subvalue}")
                            else:
                                print(f"\t{attr}: {value}")

            nodes_ids = update_nodes_ids
            time.sleep(n)

    except KeyboardInterrupt:
        logging.info("Stop collecting information of nodes.")


def partitions_collector(n:int = 10, verbose:bool = False):
    """
    Collect information of partitions of a cluster with a frequency n
    """
    try:
        partitions = pyslurm.partition()
        partitions_ids = set()
        time.sleep(5)
        while True:
            update_partitions_ids = set(partitions.ids())
            
            #new partition was created
            for partition_id in update_partitions_ids - partitions_ids:
                partition_details  = partitions.find_id(partition_id) 
                partition = Partition(**partition_details)
                logging.info(f"New partition was found: {partition}")
                logging.info(f"Collecting data of partition {partition_id}")
                if verbose:
                    attributes = partition.__dict__
                    for attr, value in attributes.items():
                            if isinstance(value, dict):
                                print(f"\t{attr}:")
                                for subattr, subvalue in value.items():
                                    print(f"\t\t{subattr}: {subvalue}")
                            else:
                                print(f"\t{attr}: {value}")
            
            # check if a partition was changed
            for partition_id in partitions_ids:
                partition_details  = partitions.find_id(partition_id) 
                if False: #partition data was modified
                    logging.info(f"Configuration of partition {partition_id} was changed")
                    logging.info(f"Updating data of partition {partition_id}")
                    if verbose:
                        for attr, value in partition_details.items():
                            if isinstance(value, dict):
                                print(f"\t{attr}:")
                                for subattr, subvalue in value.items():
                                    print(f"\t\t{subattr}: {subvalue}")
                            else:
                                print(f"\t{attr}: {value}")

            partitions_ids = update_partitions_ids
            time.sleep(n)

    except KeyboardInterrupt:
        logging.info("Stop collecting information of partitions.")

def job_collector(n:int = 1, verbose:bool = False):
    """
    Collect information of jobs submitted in a cluster with a frequency n
    """
    #import pdb; pdb.set_trace()
    try:
        jobs = pyslurm.job()

        job_ids = set(jobs.ids())
        while True: 
            updated_job_ids = set(jobs.ids())

            logging.info(f"job_ids: {job_ids}")
            logging.info(f"updated_job_ids: {updated_job_ids}")
            
            # new jobs was submmited
            for new_job_id in updated_job_ids - job_ids:
                for job_data in jobs.find_id(new_job_id):
                    new_job = Job(**job_data)
                    logging.info(f"New job was summited: {new_job}")
                    logging.info(f"Collecting data of job {new_job_id}")
                    if verbose:
                        attributes = new_job.__dict__
                        for attr, value in attributes.items():
                            if isinstance(value, dict):
                                print(f"\t{attr}:")
                                for subattr, subvalue in value.items():
                                    print(f"\t\t{subattr}: {subvalue}")
                            else:
                                print(f"\t{attr}: {value}")

            # check if data of old job was changed
            for job_id in job_ids:
                for job_data in jobs.find_id(job_id):
                    if False: #check if data of old job was changed
                        logging.info(f"Status of job {job_id} was changed")
                        logging.info(f"Updating data of job {job_id}")
                        if verbose:
                            for attr, value in job_data.items():
                                if isinstance(value, dict):
                                    print(f"\t{attr}:")
                                    for subattr, subvalue in value.items():
                                        print(f"\t\t{subattr}: {subvalue}")
                                else:
                                    print(f"\t{attr}: {value}")

            job_ids = updated_job_ids
            time.sleep(n)

    except KeyboardInterrupt:
        logging.info("Stop collecting information of jobs.")


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', help='Role of cassandra DB')
    parser.add_argument('-k', '--keyspace', help='Cassandra keyspace')
    parser.add_argument('--host', help='Hostname or IP4 of cassandra DB server')
    parser.add_argument('-v', '--verbose', action='store_true', 
                        help='Show collected data')
    parser.add_argument('-f', '--freq', nargs=3, default=[10, 10, 1],
                        help='Collection frequency (NODES, PARTITIONS, JOBS)')

    args = parser.parse_args()

    collector = []
    try:
        collector_func = [nodes_collector, partitions_collector, job_collector]
        collector_args = [(fc, args.verbose) for fc in args.freq]

        for func, args in zip(collector_func, collector_args):
            collector.append(threading.Thread(target=func, args=args))

        logging.info("Start collecting information")
        for thread_collector in collector:
            thread_collector.start()

    except KeyboardInterrupt as error:
        #import pdb; pdb.set_trace()
        logging.info("Stop collecting information.")
        if collector:
            for thread_collector in collector:
                thread_collector.join()