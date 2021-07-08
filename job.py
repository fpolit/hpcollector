#!/usr/bin/env python3
#
# Job class to collect information
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from cassandra.util import OrderedMapSerializedKey
from typing import Any, List

class Job:

    UNDEFINED = [
        "threads_per_core",
        "tres_alloc_str",
        "tres_bind",
        "tres_freq",
        "tres_per_job",
        "tres_per_node",
        "tres_per_socket",
        "tres_per_task",
        "wckey",
        "sched_nodes",
        "resv_name",
        "qos",
        "mem_per_tres",
        "network",
        "cpus_per_tres",
        "cpu_freq_gov",
        "cpu_freq_max",
        "cpu_freq_min",
        "core_spec",
        "burst_buffer",
        "burst_buffer_state",
        "batch_features",
        "batch_host",
        "array_job_id",
        "array_task_id",
        "array_task_str",
        "array_max_tasks",
        "cpus_alloc_layout" # reason: map<text, Any>
    ]

    __name__='Job'


    def __init__(self, job_id:int, name:str, job_state:str, partition:str, command:str,
                dependency:List[int] = None, **kwargs) -> None:
        self.job_id = job_id
        self.name = name
        self.job_state = job_state
        self.partition = partition
        self.command = command
        self.dependency = dependency


        for name, value in kwargs.items():
            if not hasattr(self, name) and name not in Job.UNDEFINED:
                setattr(self, name, value)
                
                if name == "time_limit":
                    self.time_limit = str(self.time_limit) 


    def __repr__(self) -> str:
        return f"Job(id={self.job_id}, name={self.name}, status={self.job_state})"


    @staticmethod
    def was_changed(job_id:int, job_data:Any, session, keyspace:str):
        old_job_data = session.execute(f"SELECT info FROM {keyspace}.jobs WHERE job_id = %s", [job_id])
        old_job = old_job_data.one().info
        updated_job = Job(**job_data)

        return not (old_job == updated_job)


    def __eq__(self, job) -> bool:
        #import pdb; pdb.set_trace()

        self_attr = self.__dict__
        job_attr = job.__dict__

        common_attr = set(job_attr.keys()).intersection(set(self_attr.keys()))

        #import pdb; pdb.set_trace()
        #print("attr\tself\t\tjob")
        for attr in common_attr:            
            if isinstance(job_attr[attr], OrderedMapSerializedKey) or isinstance(self_attr[attr], OrderedMapSerializedKey):
                job_attr[attr] = dict(job_attr[attr])
                self_attr[attr] = dict(self_attr[attr])

            if job_attr[attr] == [(), [], {}]:
                job_attr[attr] = None

            if self_attr[attr] == [(), [], {}]:
                self_attr[attr] = None

            #print(f"{attr}: {job_attr[attr]}\t\t{self_attr[attr]}")

            if self_attr[attr] != job_attr[attr]:
                return False
        return True