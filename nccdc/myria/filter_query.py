#!/usr/bin/env python

"""Filter out a relevant time range in the nccd dataset.

Store the result in a new nccdc_filtered relation
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

    insert = {
        'op_type' : 'DbInsert',
        'op_name' : 'Insert',
        'arg_child' : 'Filter2',
        'arg_overwrite_table' : True,
        'relation_key' : {
            'user_name' : 'public',
            'program_name' : 'adhoc',
            'relation_name' : 'nccdc_filtered'
        }
    }

    fragment1 = {
       'operators' : [scan, filter1, filter2, insert]
    }

    return pretty_json({
        'fragments' : [fragment1],
        'logical_ra' : 'nccdc query',
        'raw_datalog' : 'nccdc query'
    })

if __name__ == "__main__":
    print generate()
