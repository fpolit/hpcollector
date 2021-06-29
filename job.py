#!/usr/bin/env python3
#
# Job class to collect information
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from typing import List

class Job:
    ARRAY = ['array_job_id', 'array_task_id', 'array_max_tasks'] # int
    RESOURCES = ['cpus_per_socket', 'cpus_per_task'] #int

    def __init__(self, job_id:int, name:str, job_state:str, **kwargs) -> None:
        self.job_id = job_id 
        self.name = name
        self.job_state = job_state
        self.array = {}
        self.resources = {}
        self.extra = {}
        for name, value in kwargs.items():
            if not hasattr(self, name):
                if name in Job.RESOURCES:
                    self.resources[name] = value
                elif name in Job.ARRAY:
                    self.array[name] = value
                else:
                    self.extra[name] = value
    def __repr__(self) -> str:
        return f"Job(id={self.job_id}, name={self.name}, status={self.job_state})"