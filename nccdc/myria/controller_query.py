#!/usr/bin/env python

"""Identify controllers in the NCCDC dataset.

Controller is a node that talks to a node that talks to a given target"""

import json

target = '173693460:80'
input_relation_name = 'nccdc'
time_range = (1366475761, 1366475821)

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

def generate():
   scan1 = {
        'op_type' : 'TableScan',
        'op_name' : 'Scan',
        "relation_key" : {
            "user_name" : "whitaker",
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

   # TODO: store this output so we don't have to recalculate it

   # Filter out bots (flows that connect to the target)
   filter3 = {
       'op_name' : 'Filter3',
       'op_type' : 'Filter',
       'arg_child' : 'Filter2',
       'arg_predicate' : {
           'type': 'SimplePredicate',
           'arg_compare_index': 1,
           'arg_op': "EQUALS",
           'arg_compare_value' : target
       }
   }

   producer = {
       "op_name": "Shuffle",
       "op_type": "ShuffleProducer",
       "arg_child": "Filter3",
       "arg_operator_id": "hash(src)",
       "arg_pf": {
           "index": "0",
           "type": "SingleFieldHash"
       }
   }

   fragment1 = {
       'operators' : [scan1, filter1, filter2, filter3, producer]
   }

   # Join to compute controllers
   consumer = {
       "op_name": "Consumer",
       "op_type": "ShuffleConsumer",
       "arg_operator_id": "hash(src)",
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
   }

   scan2 = {
    'op_type' : 'TableScan',
       'op_name' : 'Scan2',
       "relation_key" : {
           "user_name" : "whitaker",
           "program_name" : "adhoc",
           "relation_name" : input_relation_name,
       }
   }

   filter4 = {
       'op_type' : 'Filter',
       'op_name' : 'Filter4',
       'arg_child' : 'Scan2',
       'arg_predicate' : {
           'type': 'SimplePredicate',
           'arg_compare_index': 3,
           'arg_op': "GREATER_THAN",
            'arg_compare_value' : time_range[0]
       }
   }

   filter5 = {
       'op_type' : 'Filter',
       'op_name' : 'Filter5',
       'arg_child' : 'Filter4',
       'arg_predicate' : {
           'type': 'SimplePredicate',
           'arg_compare_index': 3,
           'arg_op': "LESS_THAN",
           'arg_compare_value' : time_range[1]
       }
   }

   # Join bots with flows; project out the source column
   join = {
       "op_name": "Join",
       "op_type": "LocalJoin",
       "arg_child1": "Consumer",
       "arg_child2": "Filter5",
       "arg_columns1": [
           "0"
        ],
       "arg_columns2": [
           "1"
       ],
       "arg_select1": [
       ],
       "arg_select2": [
           "0"
        ],
   }

   insert = {
       'op_type' : 'DbInsert',
       'op_name' : 'Insert',
       'arg_child' : 'Consumer',
       'arg_overwrite_table' : True,
       'relation_key' : {
           'user_name' : 'whitaker',
           'program_name' : 'adhoc',
           'relation_name' : 'nccdc_controllers'
       }
   }

   fragment2 = {
       'operators' : [consumer, scan2, filter4, filter5, join, insert]
   }

   return pretty_json({
       'fragments' : [fragment1, fragment2],
       'logical_ra' : 'nccdc query',
       'raw_datalog' : 'nccdc query'
    })

if __name__ == "__main__":
    print generate()
