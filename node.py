#!/usr/bin/env python3
#
# Node class to collect information
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from typing import List, Any

class Node:
    RESOURCES = ['boards', 'cores', 
                'cores_per_socket', 'cpus',
                'sockets', 'threads',
                'free_mem'] #int
    OS = ['node_addr', 'os'] #str
    STATUS = ['state', 'reson'] #str
    ALLOC = ['alloc_cpus', 'alloc_mem'] #int

    def __init__(self, name:str, state:str, partitions :List[str], **kwargs) -> None:
        self.name = name
        self.state = state
        self.partitions = partitions
        self.resources = {}
        self.os = {}
        #self.status = {}
        self.alloc = {}
        self.extra = {}

        for name, value in kwargs.items():
            #if not hasattr(self, name):
            if name in Node.RESOURCES:
                self.resources[name] = value
            elif name in Node.OS:
                self.os[name] = value
            # elif name in Node.STATUS:
            #     self.status[name] = value
            elif name in Node.ALLOC:
                self.alloc[name] = value
            else:
                self.extra[name] = value

    @staticmethod
    def was_changed(node_id:int, node_data:Any, session, keyspace:str):
        old_node_data = session.execute(f"SELECT info FROM {keyspace}.nodes WHERE name = %s", [node_id])
        old_node = old_node_data.one().info
        updated_node = Node(**node_data)
        #import pdb; pdb.set_trace()
        if old_node == updated_node:
            return False
        return True

    def __repr__(self) -> str:
        return f"Node(name={self.name}, status={self.state}, partitions={self.partitions})"

    def __eq__(self, node: object) -> bool:
        return self.name == node.name \
                and self.state == node.state \
                and self.partitions == node.partitions