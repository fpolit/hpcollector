#!/usr/bin/env python3
#
# Partition class to collect information
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from cassandra.util import OrderedMapSerializedKey
from typing import List, Any

class Partition:
    UNDEFINED = [
        "deny_accounts",
        "deny_qos",
        "alternate",
        "billing_weights_str",
        "def_mem_per_cpu",
        "flags", # dictionary with diferent value types
        "max_mem_per_cpu",
        "qos_char",
    ]

    def __init__(self, name:str, nodes :str, state:str, **kwargs) -> None:
        self.name = name
        self.state = state
        self.nodes = nodes
        
        for name, value in kwargs.items():
            if not hasattr(self, name) and name not in Partition.UNDEFINED:
                setattr(self, name, value)
                #print(f"{name}\t{type(value)}\t{value}")
        #import pdb; pdb.set_trace()


    @staticmethod
    def was_changed(partition_id:int, partition_data:Any, session, keyspace:str):
        old_partition_data = session.execute(f"SELECT info FROM {keyspace}.partitions WHERE name = %s", [partition_id])
        #import pdb; pdb.set_trace()
        old_partition = old_partition_data.one().info
        updated_partition = Partition(**partition_data)
        
        result =  not(old_partition == updated_partition)
        return result

    def __repr__(self) -> str:
        return f"Partition(name={self.name}, status={self.state}, nodes={self.nodes})"

    def __eq__(self, partition: object) -> bool:
        self_attr = self.__dict__
        partition_attr = partition.__dict__

        common_attr = set(partition_attr.keys()).intersection(set(self_attr.keys()))

        #import pdb; pdb.set_trace()
        for attr in common_attr:
            if isinstance(partition_attr[attr], OrderedMapSerializedKey) or isinstance(self_attr[attr], OrderedMapSerializedKey):
                partition_attr[attr] = dict(partition_attr[attr])
                self_attr[attr] = dict(self_attr[attr])

            if self_attr[attr] != partition_attr[attr]:
                return False
        return True