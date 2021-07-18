#!/usr/bin/env python3
#
# Table jobs
#
# Maintainer: glozanoa <glozanoa@uni.pe>

import uuid
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

# UNDEFINED = [
#     "threads_per_core",
#     "tres_alloc_str",
#     "tres_bind",
#     "tres_freq",
#     "tres_per_job",
#     "tres_per_node",
#     "tres_per_socket",
#     "tres_per_task",
#     "wckey",
#     "sched_nodes",
#     "resv_name",
#     "qos",
#     "mem_per_tres",
#     "network",
#     "cpus_per_tres",
#     "cpu_freq_gov",
#     "cpu_freq_max",
#     "cpu_freq_min",
#     "core_spec",
#     "burst_buffer",
#     "burst_buffer_state",
#     "batch_features",
#     "batch_host",
#     "array_job_id",
#     "array_task_id",
#     "array_task_str",
#     "array_max_tasks",
#     "cpus_alloc_layout" # reason: map<text, Any>
# ]

class Jobs(Model):
    job_id                  = Integer(primary_key=True)
    name                    = Text()
    job_state               = Text(required=True)
    partition               = Text(required=True)
    command                 = Text()
    account                 = Text()
    accrue_time             = Text()
    admin_comment           = Text()
    alloc_node              = Text()
    alloc_sid               = Integer()
    assoc_id                = Integer()
    batch_flag              = Integer()
    billable_tres           = Text()
    bitflags                = Text()
    boards_per_node         = Integer()
    comment                 = Text()
    contiguous              = Boolean()
    cores_per_socket        = Integer()
    cpus_per_task           = Integer()
    derived_ec              = Text()
    eligible_time           = BigInt()
    end_time                = BigInt()
    exc_nodes               = List(value_type=Text())
    exit_code               = Text()
    features                = List(value_type=Text())
    group_id                = Integer()
    last_sched_eval	        = Text()
    licenses                = Map(key_type=Text(), value_type=Text())
    max_cpus                = Integer()
    max_nodes               = Integer()
    nodes                   = Text()
    nice                    = Integer()
    ntasks_per_core         = Integer()
    ntasks_per_core_str     = Text()
    ntasks_per_node         = Integer()
    ntasks_per_socket       = Integer()
    ntasks_per_socket_str   = Text()
    ntasks_per_board        = Integer()
    num_cpus                = Integer()
    num_nodes               = Integer()
    num_tasks               = Integer()
    mem_per_cpu             = Boolean()
    min_memory_cpu          = Text()
    mem_per_node            = Boolean()
    min_memory_node         = Integer()
    pn_min_memory           = Integer()
    pn_min_cpus             = Integer()
    pn_min_tmp_disk         = Integer()
    power_flags             = Integer()
    priority                = Text()
    profile                 = Text()
    reboot                  = Integer()
    req_nodes               = List(value_type=Text())
    req_switch              = Integer()
    requeue                 = Boolean()
    resize_time             = Integer()
    restart_cnt             = Integer()
    run_time                = Integer()
    run_time_str            = Text()
    shared                  = Text()
    show_flags              = Integer()
    sockets_per_board       = Integer()
    sockets_per_node        = Integer()
    start_time              = BigInt()
    state_reason            = Text()
    std_err                 = Text()
    std_in                  = Text()
    std_out                 = Text()
    submit_time             = BigInt()
    suspend_time            = Integer()
    system_comment          = Text()
    time_limit              = Text()
    time_min                = Integer()
    tres_req_str            = Text()
    user_id                 = Integer()
    wait4switch             = Integer()
    work_dir                = Text()
    cpus_allocated          = Map(key_type=Text(), value_type=Integer())


    @staticmethod
    def purge_args(**kwargs):
        cols = Jobs().keys()
        purged_args = {}
        for name, value in kwargs.items():
            if name in cols:
                if name in ['time_limit', 'priority', 'profile', 'billable_tres', 'bitflags']:
                    value = str(value)
                
                purged_args[name] = value

                
        
        return purged_args

    @staticmethod
    def updated_columns(old_job, updated_job):
        dict_old_job = dict(old_job)
        dict_updated_job = dict(updated_job)

        common_keys = set(dict_old_job.keys()).intersection(set(dict_updated_job))

        updated_cols = {}
        for key in common_keys:
            if dict_old_job[key] != dict_updated_job[key]:
                updated_cols[key] = dict_updated_job[key]
            
        return updated_cols
