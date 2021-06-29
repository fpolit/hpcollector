#!/usr/bin/env python3
#
# Node class to collect information
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from typing import List

class Node:
    RESOURCES = ['boards', 'cores', 
                'cores_per_socket', 'cpus',
                'sockets', 'threads',
                'free_mem'] #int
    OS = ['node_addr', 'os', 'boot_time'] #str
    STATUS = ['state', 'reson'] #str
    ALLOC = ['alloc_cpus', 'alloc_mem'] #int

    def __init__(self, name:str, partitions :List[str], state:str = 'DOWN', **kwargs) -> None:
        self.name = name
        self.state = state
        self.partitions = partitions
        self.resources = {}
        self.os = {}
        self.status = {}
        self.alloc = {}
        self.extra = {}

        for name, value in kwargs.items():
            if not hasattr(self, name):
                if name in Node.RESOURCES:
                    self.resources[name] = value
                elif name in Node.OS:
                    self.os[name] = value
                elif name in Node.STATUS:
                    self.status[name] = value
                elif name in Node.ALLOC:
                    self.alloc[name] = value
                else:
                    self.extra[name] = value
                    
    def __repr__(self) -> str:
        return f"Node(name={self.name}, status={self.state}, partitions={self.partitions})"