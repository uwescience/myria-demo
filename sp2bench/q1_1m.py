#!/usr/bin/env python

"""Filter out a relevant time range in the nccd dataset.

Store the result in a new nccdc_filtered relation
"""

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
            'arg_compare_value' : '"Journal_1_(1940)"^^xsd:string'
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
            'arg_compare_value' : 'dc:title'
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

    filter2a = {
        'op_type' : 'Filter',
        'op_name' : 'Filter2a',
        'arg_child' : 'Scan2',
        'arg_predicate' : {
            'type': 'SimplePredicate',
            'arg_compare_index': 2,
            'arg_op': "EQUALS",
            'arg_compare_value' : 'bench:Journal'
        }
    }

    filter2b = {
        'op_type' : 'Filter',
        'op_name' : 'Filter2b',
        'arg_child' : 'Filter2a',
        'arg_predicate' : {
            'type': 'SimplePredicate',
            'arg_compare_index': 1,
            'arg_op': "EQUALS",
            'arg_compare_value' : 'rdf:type'
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
        "arg_child": "Filter2b",
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
        'operators' : [scan2, filter2a, filter2b, producer2]
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

    producer3 = {
        "op_name": "Shuffle3",
        "op_type": "ShuffleProducer",
        "arg_child": "Join1",
        "arg_operator_id": "producer3",
        "arg_pf": {
            "index": "0",
            "type": "SingleFieldHash"
        }
    }

    fragment3 = {
        'operators' : [consumer1, consumer2, join1, producer3]
    }

    scan3 = {
        'op_type' : 'TableScan',
        'op_name' : 'Scan3',
        "relation_key" : {
            "user_name" : "public",
            "program_name" : "adhoc",
            "relation_name" : "sp2bench_1m",
        }
    }

    filter3 = {
        'op_type' : 'Filter',
        'op_name' : 'Filter3',
        'arg_child' : 'Scan3',
        'arg_predicate' : {
            'type': 'SimplePredicate',
            'arg_compare_index': 1,
            'arg_op': "EQUALS",
            'arg_compare_value' : 'dcterms:issued'
        }
    }

    producer4 = {
        "op_name": "Shuffle4",
        "op_type": "ShuffleProducer",
        "arg_child": "Filter3",
        "arg_operator_id": "producer4",
        "arg_pf": {
            "index": "0",
            "type": "SingleFieldHash"
        }
    }

    fragment4 = {
        'operators' : [scan3, filter3, producer4]
    }

    consumer3 = {
        "op_name": "Consumer3",
        "op_type": "ShuffleConsumer",
        "arg_operator_id": "producer3",
        "arg_schema": {
           "column_names": [
               "subject"
           ],
           "column_types": [
               "STRING_TYPE"
           ]
        }
    }

    consumer4 = {
        "op_name": "Consumer4",
        "op_type": "ShuffleConsumer",
        "arg_operator_id": "producer4",
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

    join2 = {
        "op_name": "Join2",
        "op_type": "LocalJoin",
        "arg_child1": "Consumer3",
        "arg_child2": "Consumer4",
        "arg_columns1": [
            "0"
        ],
        "arg_columns2": [
            "0"
        ],
        "arg_select1": [
        ],
        "arg_select2": [
            "2"
        ]
    }

    insert = {
        'op_type' : 'DbInsert',
        'op_name' : 'Insert',
        'arg_child' : 'Join2',
        'arg_overwrite_table' : True,
        'relation_key' : {
            'user_name' : 'public',
            'program_name' : 'adhoc',
            'relation_name' : 'sp2bench_1m_q1'
        }
    }

    fragment5 = {
        'operators' : [consumer3, consumer4, join2, insert]
    }

    return pretty_json({
        'fragments' : [fragment1, fragment2, fragment3, fragment4, fragment5],
        'logical_ra' : 'sp2bench Q1',
        'raw_datalog' : 'sp2bench Q1'
    })

if __name__ == "__main__":
    print generate()
