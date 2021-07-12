#!/usr/bin/env python3
#
# Table jobs
#
# Maintainer: glozanoa <glozanoa@uni.pe>

import uuid
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

# UNDEFINED = [
#     "deny_accounts",
#     "deny_qos",
#     "alternate",
#     "billing_weights_str",
#     "def_mem_per_cpu",
#     "flags", # dictionary with diferent value types
#     "max_mem_per_cpu",
#     "qos_char",
# ]

class Partitions(Model):
    name                = Text(primary_key=True)
    state               = Text()
    nodes               = Text()
    allow_accounts      = Text()
    allow_alloc_nodes   = Text()
    allow_groups        = Text()
    allow_qos           = Text()
    cr_type             = Integer()
    def_mem_per_node    = Text()
    default_time        = Text()
    default_time_str    = Text()
    grace_time          = Integer()
    max_cpus_per_node   = Text()
    max_mem_per_node    = Text()
    max_nodes           = Text()
    max_share           = Integer()
    max_time            = Text()
    max_time_str        = Text()
    min_nodes           = Integer()
    over_time_limit     = Text()
    preempt_mode        = Text()
    priority_job_factor = Integer()
    priority_tier       = Integer()
    total_cpus          = Integer()
    total_nodes         = Integer()
    tres_fmt_str        = Text()

    @staticmethod
    def purge_args(**kwargs):
        cols = Partitions().keys()
        purged_args = {}
        for name, value in kwargs.items():
            if name in cols:
                purged_args[name] = value

                
        
        return purged_args

    @staticmethod
    def updated_columns(old_partition, updated_partition):
        dict_old_partition = dict(old_partition)
        dict_updated_partition = dict(updated_partition)

        common_keys = set(dict_old_partition.keys()).intersection(set(dict_updated_partition))

        updated_cols = {}
        for key in common_keys:
            if dict_old_partition[key] != dict_updated_partition[key]:
                updated_cols[key] = dict_updated_partition[key]
            
        return updated_cols
