#!/usr/bin/env python

"""sp2bench q3a"""

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

def generate():
    scan1 = {
        'op_type' : 'TableScan',
        'op_name' : 'Scan1',
        "relation_key" : {
            "user_name" : "public",
            "program_name" : "adhoc",
            "relation_name" : "sp2bench_1m",
        }
    }

    filter1a = {
        'op_type' : 'Filter',
        'op_name' : 'Filter1a',
        'arg_child' : 'Scan1',
        'arg_predicate' : {
            'type': 'SimplePredicate',
            'arg_compare_index': 2,
            'arg_op': "EQUALS",
            'arg_compare_value' : 'bench:Article'
        }
    }

    filter1b = {
        'op_type' : 'Filter',
        'op_name' : 'Filter1b',
        'arg_child' : 'Filter1a',
        'arg_predicate' : {
            'type': 'SimplePredicate',
            'arg_compare_index': 1,
            'arg_op': "EQUALS",
            'arg_compare_value' : 'rdf:type'
        }
    }

    scan2 = {
        'op_type' : 'TableScan',
        'op_name' : 'Scan2',
        "relation_key" : {
            "user_name" : "public",
            "program_name" : "adhoc",
            "relation_name" : "sp2bench_1m",
        }
    }

    filter2 = {
        'op_type' : 'Filter',
        'op_name' : 'Filter2',
        'arg_child' : 'Scan2',
        'arg_predicate' : {
            'type': 'SimplePredicate',
            'arg_compare_index': 1,
            'arg_op': "EQUALS",
            'arg_compare_value' : 'swrc:pages'
        }
    }

    producer1 = {
        "op_name": "Shuffle1",
        "op_type": "ShuffleProducer",
        "arg_child": "Filter1b",
        "arg_operator_id": "producer1",
        "arg_pf": {
            "index": "0",
            "type": "SingleFieldHash"
        }
    }

    producer2 = {
        "op_name": "Shuffle2",
        "op_type": "ShuffleProducer",
        "arg_child": "Filter2",
        "arg_operator_id": "producer2",
        "arg_pf": {
            "index": "0",
            "type": "SingleFieldHash"
        }
    }

    fragment1 = {
        'operators' : [scan1, filter1a, filter1b, producer1]
    }

    fragment2 = {
        'operators' : [scan2, filter2, producer2]
    }

    consumer1 = {
        "op_name": "Consumer1",
        "op_type": "ShuffleConsumer",
        "arg_operator_id": "producer1",
        "arg_schema": {
           "column_names": [
               "subject",
               "predicate",
               "object"
           ],
           "column_types": [
               "STRING_TYPE",
               "STRING_TYPE",
               "STRING_TYPE"
           ]
        }
    }

    consumer2 = {
        "op_name": "Consumer2",
        "op_type": "ShuffleConsumer",
        "arg_operator_id": "producer2",
        "arg_schema": {
           "column_names": [
               "subject",
               "predicate",
               "object"
           ],
           "column_types": [
               "STRING_TYPE",
               "STRING_TYPE",
               "STRING_TYPE"
           ]
        }
    }

    join1 = {
        "op_name": "Join1",
        "op_type": "LocalJoin",
        "arg_child1": "Consumer1",
        "arg_child2": "Consumer2",
        "arg_columns1": [
            "0"
        ],
        "arg_columns2": [
            "0"
        ],
        "arg_select1": [
            "0"
        ],
        "arg_select2": [
        ]
    }

    insert = {
        'op_type' : 'DbInsert',
        'op_name' : 'Insert',
        'arg_child' : 'Join1',
        'arg_overwrite_table' : True,
        'relation_key' : {
            'user_name' : 'public',
            'program_name' : 'adhoc',
            'relation_name' : 'sp2bench_1m_q3a'
        }
    }

    fragment3 = {
        'operators' : [consumer1, consumer2, join1, insert]
    }

    return pretty_json({
        'fragments' : [fragment1, fragment2, fragment3],
        'logical_ra' : 'sp2bench Q3a',
        'raw_datalog' : 'sp2bench Q3a'
    })

if __name__ == "__main__":
    print generate()
