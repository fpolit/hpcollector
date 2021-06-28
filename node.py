#!/usr/bin/env python3
#
# Node class to colect information
#
# Maintainer: glozanoa <glozanoa@uni.pe>

from typing import List

class Node:
    def __init__(self, name:str, partitions :List[str], state:str = 'DOWN', **kwargs) -> None:
        self.name = name
        self.state = state
        self.partitions = partitions
        self.information = kwargs
        for name, value in kwargs.items():
            if not hasattr(self, name):
                setattr(self, name, value)
        
    def __repr__(self) -> str:
        return f"Node(name={self.name}, status={self.state}, partitions={self.partitions})"