#!/usr/bin/env python

"""Identify DDOS victims within a given time window

Roughly:
SELECT dst FROM nccdc GROUP BY dst HAVING COUNT(src) > 10000;
"""

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

attack_threshold = 10000
time_range = (1366475761, 1366475821)
input_relation_name = 'nccdc'

def generate():
    scan = {
        'op_type' : 'TableScan',
        'op_name' : 'Scan',
        "relation_key" : {
            "user_name" : "public",
            "program_name" : "adhoc",
            "relation_name" : input_relation_name,
        }
    }

    filter1 = {
        'op_type' : 'Filter',
        'op_name' : 'Filter1',
        'arg_child' : 'Scan',
        'arg_predicate' : {
            'type': 'SimplePredicate',
            'arg_compare_index': 3,
            'arg_op': "GREATER_THAN",
            'arg_compare_value' : time_range[0]
        }
    }

    filter2 = {
        'op_type' : 'Filter',
        'op_name' : 'Filter2',
        'arg_child' : 'Filter1',
        'arg_predicate' : {
            'type': 'SimplePredicate',
            'arg_compare_index': 3,
            'arg_op': "LESS_THAN",
            'arg_compare_value' : time_range[1]
        }
    }

    producer = {
        "op_name": "Shuffle",
        "op_type": "ShuffleProducer",
        "arg_child": "Filter2",
        "arg_operator_id": "1",
        "arg_pf": {
            "index": "1",
            "type": "SingleFieldHash"
        },
    }

    fragment1 = {
        'operators' : [scan, filter1, filter2, producer]
    }

    consumer =  {
        "arg_operator_id": "1",
        "arg_schema": {
            "column_names": [
                "src",
                "dst",
                "proto",
                "time",
                "col4",
                "col5",
                "col6"
            ],
            "column_types": [
                "STRING_TYPE",
                "STRING_TYPE",
                "INT_TYPE",
                "INT_TYPE",
                "INT_TYPE",
                "INT_TYPE",
                "INT_TYPE"
            ]
        },
        "op_name": "Gather",
        "op_type": "ShuffleConsumer"
    }

    groupby = {
        "op_name": "GroupBy",
        "op_type": "SingleGroupByAggregate",
        "arg_child": "Gather",

        "arg_agg_fields": [
            "0"
        ],
        "arg_agg_operators": [
            [
                "AGG_OP_COUNT"
            ]
        ],

        "arg_group_field": "1",
    }


    # Filter out only those victims with many in-bound flows
    filter3 = {
        'op_type' : 'Filter',
        'op_name' : 'Filter3',
        'arg_child' : 'GroupBy',
        'arg_predicate' : {
            'type': 'SimplePredicate',
            'arg_compare_index': 1,
            'arg_op': "GREATER_THAN",
            'arg_compare_value' : attack_threshold
        }
    }

    insert = {
        'op_type' : 'DbInsert',
        'op_name' : 'Insert',
        'arg_child' : 'Filter3',
        'arg_overwrite_table' : True,
        'relation_key' : {
            'user_name' : 'public',
            'program_name' : 'adhoc',
            'relation_name' : 'nccdc_ddos_victims'
        }
    }

    fragment2 = {
        'operators' : [consumer, groupby, filter3, insert]
    }

    return pretty_json({
        'fragments' : [fragment1, fragment2],
        'logical_ra' : 'nccdc query',
        'raw_datalog' : 'nccdc query'
    })

if __name__ == "__main__":
    print generate()
