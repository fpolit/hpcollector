#!/usr/bin/env python3
#
# Table jobs
#
# Maintainer: glozanoa <glozanoa@uni.pe>

import uuid
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

# UNDEFINED = [
#     "gres_used",
#     "mcs_label",
#     "owner",
#     "reason",
#     "reason_time",
#     "power_mgmt"
# ]

class Nodes(Model):
    name                = Text(primary_key=True)
    state               = Text()
    partitions          = List(value_type=Text())
    arch                = Text()
    boards              = Integer()
    boot_time           = Integer()
    cores               = Integer()
    core_spec_cnt       = Integer()
    cores_per_socket    = Integer()
    cpus                = Integer()
    cpu_load            = Integer()
    cpu_spec_list       = List(value_type=Text())
    features            = List(value_type=Text())
    features_active     = List(value_type=Text())
    free_mem            = Integer()
    gres                = List(value_type=Text())
    gres_drain          = Text()
    mem_spec_limit      = Integer()
    node_addr           = Text()
    node_hostname       = Text()
    os                  = Text()
    real_memory         = Integer()
    slurmd_start_time   = Integer()
    sockets             = Integer()
    threads             = Integer()
    tmp_disk            = Integer()
    weight              = Integer()
    tres_fmt_str        = Text()
    version             = Text()
    reason_uid          = Integer()
    #power_mgmt          = Map(key_type=Text(), value_type=Integer())
    energy              = Map(key_type=Text(), value_type=Integer())
    alloc_cpus          = Integer()
    err_cpus            = Integer()
    alloc_mem           = Integer()


    @staticmethod
    def purge_args(**kwargs):
        cols = Nodes().keys()
        purged_args = {}
        for name, value in kwargs.items():
            if name in cols:
                purged_args[name] = value

                
        
        return purged_args

    @staticmethod
    def updated_columns(old_node, updated_node):
        dict_old_node = dict(old_node)
        dict_updated_node = dict(updated_node)

        common_keys = set(dict_old_node.keys()).intersection(set(dict_updated_node))

        updated_cols = {}
        for key in common_keys:
            if dict_old_node[key] != dict_updated_node[key]:
                updated_cols[key] = dict_updated_node[key]
            
        return updated_cols
