#!/usr/bin/env python3
#
# Job class to collect information
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from typing import Any, List

class Job:
    ARRAY = ['array_job_id', 'array_task_id', 'array_max_tasks'] # int
    RESOURCES = ['cpus_per_socket', 'cpus_per_task'] #int

    def __init__(self, job_id:int, name:str, job_state:str, partition:str, command:str,
                dependency:List[int] = [], **kwargs) -> None:
        self.job_id = job_id 
        self.name = name
        self.job_state = job_state
        self.partition = partition
        self.command = command
        self.array = {}
        self.resources = {}
        self.dependency = dependency
        self.extra = {}
        for name, value in kwargs.items():
            #if not hasattr(self, name):
            if name in Job.RESOURCES:
                self.resources[name] = value
            elif name in Job.ARRAY:
                self.array[name] = value
            else:
                self.extra[name] = value

    def __repr__(self) -> str:
        return f"Job(id={self.job_id}, name={self.name}, status={self.job_state})"

    @staticmethod
    def was_changed(job_id:int, job_data:Any, session, keyspace:str):
        old_job_data = session.execute(f"SELECT info FROM {keyspace}.jobs WHERE job_id = %s", [job_id])
        #import pdb; pdb.set_trace()
        old_job = old_job_data.one().info
        updated_job = Job(**job_data)

        return old_job != updated_job

    def __eq__(self, job) -> bool:
        return self.job_id == job.job_id \
                and self.name == job.name \
                and self.job_state == job.job_state \
                and self.partition == job.partition \
                and self.command == job.command

    def __ne__(self, job: object) -> bool:
        return not (self == job)