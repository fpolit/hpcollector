#!/usr/bin/env python3
#
# Collect data of nodes, partitions and job of a cluster
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from typing import Any, List
import threading
import time
import logging


from node import Node

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')

try:
    import pyslurm
except ModuleNotFoundError as error:
    logging.critical("No PySlurm package installed")
    exit(1)


def nodes_collector(n:int = 10):
    """
    Collect information of nodes of a cluster with a frequency n
    """
    try:
        nodes = pyslurm.node()
        nodes_ids = set()
        while True:
            update_nodes_ids = set(nodes.ids())
            
            # check if new nodes was added
            for node_id in update_nodes_ids - nodes_ids:
                if True: #new partition was created
                    node_details = nodes.find_id(node_id)
                    node = Node(**node_details)
                    logging.info(f"New node was found: {node}")
                    logging.info(f"Collecting data of node {node_id}")
            
            # check if a partition was changed
            for node_id in nodes_ids:
                node_details  = nodes.find_id(node_id)
                if False: #partition data was modified
                    logging.info(f"Configuration of node {node_id} was changed")
                    logging.info(f"Updating data of node {node_id}")

            nodes_ids = update_nodes_ids
            time.sleep(n)

    except KeyboardInterrupt:
        logging.info("Stop collecting information of nodes.")


def partitions_collector(n:int = 10):
    """
    Collect information of partitions of a cluster with a frequency n
    """
    try:
        partitions = pyslurm.partition()
        partitions_ids = set()
        while True:
            update_partitions_ids = set(partitions.ids())
            
            # check if new partitions was created
            for partition_id in update_partitions_ids - partitions_ids:
                if True: #new partition was created
                    logging.info(f"New partition was found: {partition_id}")
                    logging.info(f"Collecting data of partition {partition_id}")
            
            # check if a partition was changed
            for partition_id in partitions_ids:
                partition_details  = partitions.find_id(partition_id) 
                if False: #partition data was modified
                    logging.info(f"Configuration of partition {partition_id} was changed")
                    logging.info(f"Updating data of partition {partition_id}")

            partitions_ids = update_partitions_ids
            time.sleep(n)

    except KeyboardInterrupt:
        logging.info("Stop collecting information of partitions.")

def job_collector(n:int = 1):
    """
    Collect information of jobs submitted in a cluster with a frequency n
    """
    #import pdb; pdb.set_trace()
    try:
        jobs = pyslurm.job()

        job_ids = set(jobs.ids())
        while True: 
            updated_job_ids = set(jobs.ids())
            
            # check if new jobs was submmited
            for new_job_id in updated_job_ids - job_ids:
                for job_data in jobs.find_id(new_job_id):
                    logging.info(f"New job was summited: {new_job_id}")
                    logging.info(f"Collecting data of job {new_job_id}")

            # check if data of old job was changed
            for job_id in job_ids:
                for job_data in jobs.find_id(job_id):
                    if False: #check if data of old job was changed
                        logging.info(f"Status of job {job_id} was changed")
                        logging.info(f"Updating data of job {job_id}")

            job_ids = updated_job_ids
            time.sleep(n)

    except KeyboardInterrupt:
        logging.info("Stop collecting information of jobs.")


if __name__=="__main__":
    collector = []
    try:
        collector_func = [nodes_collector, partitions_collector, job_collector]
        collector_args = [(10,), (10,), (1,)]

        for func, args in zip(collector_func, collector_args):
            collector.append(threading.Thread(target=func, args=args))

        logging.info("Start collecting information")
        for thread_collector in collector:
            thread_collector.start()

    except KeyboardInterrupt as error:
        import pdb; pdb.set_trace()
        logging.info("Stop collecting information.")
        if collector:
            for thread_collector in collector:
                thread_collector.join()