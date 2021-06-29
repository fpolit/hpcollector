#!/usr/bin/env python3
#
# Partition class to collect information
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from typing import List

class Partition:
    ACCOUNT = ['allows_accounts', 'deny_accounts', 'allow_alloc_nodes','allow_groups', 'allow_qos'] #str
    RESOURCES = ['total_cpus', 'total_nodes'] # int
    RESTRICTIONS = ['def_mem_per_cpu', 'def_mem_per_node',
					'default_time', 'max_cpus_per_node',
					'max_mem_per_cpu', 'max_mem_per_node',
					'max_nodes', 'max_time'] # str
    PRIORITIES = ['priority_job_factor', 'priority_tier'] #int
    def __init__(self, name:str, nodes :str, state:str = 'DOWN', **kwargs) -> None:
        self.name = name
        self.state = state
        self.nodes = nodes
        self.account = {}
        self.resources = {}
        self.restrictions = {}
        self.priorities = {}
        self.extra = {}

        for name, value in kwargs.items():
            if not hasattr(self, name):
                if name in Partition.ACCOUNT:
                    self.account[name] = value
                elif name in Partition.RESTRICTIONS:
                    self.restrictions[name] = value
                elif name in Partition.PRIORITIES:
                    self.priorities[name] = value
                else:
                    self.extra[name] = value
        
    def __repr__(self) -> str:
        return f"Partition(name={self.name}, status={self.state}, nodes={self.nodes})"