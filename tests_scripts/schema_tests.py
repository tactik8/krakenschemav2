
from krakenschema.schema import Schema
import inspect


class Tests_schema:
    def __init__(self):
        a=1
        
    def test_post():
        a=1
        
    def test_replace_value(self):

        input_record = {
            'key1': 'value1',
            'key2': 'value2',
            'key3': {
                'key31': 'value31',
                'key32': 'value32'
            },
            'key4': [
                {'key41': 'value41'},
                {'key42': 'value42'}
            ]

        }

        expected_record = {
                        'key1': 'value1',
            'key2': 'value2',
            'key3': {
                'key31': 'value31',
                'key32': 'valueNEW',
            },
            'key4': [
                {'key41': 'value41'},
                {'key42': 'value42'}
            ]

        }

        schema = Schema()
        output_record = schema.replace_value(input_record, 'key32', 'value32', 'valueNEW' )
        
        fn_name = inspect.stack()[0][3]

        if output_record == expected_record:
            print(fn_name, 'pass')
        else: 
                print(fn_name, 'fail')
                print(output_record)

    def test_diff_same(self):

        record1 = {
                        'key1': 'value1',
            'key2': 'value2',
            'key3': {
                'key31': 'value31',
                'key32': 'valueNEW',
            },
            'key4': [
                {'key41': 'value41'},
                {'key42': 'value42'}
            ]

        }

        record2 = {
                        'key1': 'value1',
            'key2': 'value2',
            'key3': {
                'key31': 'value31',
                'key32': 'valueNEW',
            },
            'key4': [
                {'key41': 'value41'},
                {'key42': 'value42'}
            ]

        }

        schema = Schema()

        schema.record = record1
        schema.ref_record = record2

        schema._get_delta()
        new_record = schema.delta_record

        