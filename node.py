#!/usr/bin/env python3
#
# Node class to collect information
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from cassandra.util import OrderedMapSerializedKey
from typing import List, Any

class Node:
    UNDEFINED = [
        "gres_used",
        "mcs_label",
        "owner",
        "reason",
        "reason_time"
    ]

    __name__='Node'

    def __init__(self, name:str, state:str, partitions :List[str], **kwargs) -> None:
        self.name = name
        self.state = state
        self.partitions = partitions


        #import pdb; pdb.set_trace()
        for name, value in kwargs.items():
            if not hasattr(self, name) and name not in Node.UNDEFINED:
                setattr(self, name, value)



    @staticmethod
    def was_changed(node_id:int, node_data:Any, session, keyspace:str):

        old_node_data = session.execute(f"SELECT info FROM {keyspace}.nodes WHERE name = %s", [node_id])
        old_node = old_node_data.one().info
        updated_node = Node(**node_data)
        
        return not (old_node == updated_node)

    def __repr__(self) -> str:
        return f"Node(name={self.name}, status={self.state}, partitions={self.partitions})"

    def __eq__(self, node: object) -> bool:
        self_attr = self.__dict__
        node_attr = node.__dict__

        common_attr = set(node_attr.keys()).intersection(set(self_attr.keys()))

        #import pdb; pdb.set_trace()
        for attr in common_attr:
            if isinstance(node_attr[attr], OrderedMapSerializedKey) or isinstance(self_attr[attr], OrderedMapSerializedKey):
                node_attr[attr] = dict(node_attr[attr])
                self_attr[attr] = dict(self_attr[attr])

            if self_attr[attr] != node_attr[attr]:
                return False
        return True