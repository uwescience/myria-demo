#!/usr/bin/env python

"""
Filter out a relevant time range in the nccdc dataset.
Project out only the src and dst fields.
Eliminate duplicates.
Store the result in a new nccdc_filtered_distinct relation.

nccdc_filtered_distinct(src,dst) :- nccdc(src, dst, timestamp, _,_,_,_),
                                    timestamp > 1366475761,
                                    timestamp < 136647582
"""

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

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

    project = {
        'op_type' : 'Project',
        'op_name' : 'Project',
        'arg_child' : 'Filter2',
        'arg_field_list' : ["0","1"]
    }


    producer = {
        "op_name": "Shuffle",
        "op_type": "ShuffleProducer",
        "arg_child": "Project",
        "arg_operator_id": "op1",
        "arg_pf": {
            "index": [
                "0",
                "1"
            ],
            'type' : 'MultiFieldHash'
        }
    }

    fragment1 = {
       'operators' : [scan, filter1, filter2, project, producer]
    }

    consumer = {
        "op_name": "Consumer",
        "op_type": "ShuffleConsumer",
        "arg_operator_id": "op1",
        "arg_schema": {
            "column_names": [
                "src",
                "dst"
            ],
            "column_types": [
               "STRING_TYPE",
                "STRING_TYPE"
            ]
        }
    }

    dupelim = {
        'op_name' : 'DupElim',
        'op_type' : 'DupElim',
        'arg_child' : 'Consumer'
    }

    insert = {
        'op_type' : 'DbInsert',
        'op_name' : 'Insert',
        'arg_child' : 'DupElim',
        'arg_overwrite_table' : True,
        'relation_key' : {
            'user_name' : 'public',
            'program_name' : 'adhoc',
            'relation_name' : 'nccdc_filtered_distinct'
        }
    }

    fragment2 = {
        'operators' : [consumer, dupelim, insert]
    }

    return pretty_json({
        'fragments' : [fragment1, fragment2],
        'logical_ra' : '???',
        'raw_datalog' :
        'nccdc_filtered_distinct(src,dst) :- ' +
        'nccdc(src, dst, timestamp, _,_,_,_), ' +
        'timestamp > 1366475761, timestamp < 136647582'
    })

if __name__ == "__main__":
    print generate()
