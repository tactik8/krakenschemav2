    
from krakenschema.schema import Schema
import inspect


    
def test_replace_value():

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

    assert(output_record) == expected_record


def test_schema_diff_same():

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

    assert(new_record) == {}



def test_schema_diff_null1():

    record1 = {}

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

    assert(new_record) == {}


def test_schema_diff_null2():


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
    record2 = {}


    schema = Schema()

    schema.record = record1
    schema.ref_record = record2

    schema._get_delta()
    new_record = schema.delta_record

    assert(new_record) == record1


def test_schema_diff_null3():


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
        'key2': 'value2'
    }

    ref_record = {
        'key1': 'value1',
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

    assert(new_record) == ref_record




def test_schema_diff_almost_identical():

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
        'key1': 'valueNEW',
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

    ref_record = {
        'key1': 'value1'
    }

    schema = Schema()

    schema.record = record1
    schema.ref_record = record2

    schema._get_delta()
    new_record = schema.delta_record

    assert(new_record) == ref_record


def test_schema_diff_credibility():

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

    metadata1 = {
        'key1': {
            'kraken:credibility': 50
        }
    }


    record2 = {
        'key1': 'valueNEW',
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

    metadata2 = {
        'key1': {
            'kraken:credibility': 60
        }
    }


    ref_record = {
        'key1': 'valueNEW'
    }

    schema = Schema()

    schema.record = record1
    schema.ref_record = record2
    schema.metadata = metadata1
    schema.ref_metadata = metadata2

    schema._get_delta()
    new_record = schema.delta_record

    assert(new_record) == ref_record


def test_schema_get_main_record():

    input_record = { 
            '@type': 'schema:test',
            '@id': 'test_id',
            'schema:name': 'Test record',
            'schema:url': 'https://www.test.com',
            'schema:address': {
                '@type': 'schema:postaladdress',
                '@id': 'addr1',
                'schema:streetaddress': '269 de Carignan',
                'schema:address:locality': 'Repentigny',
                'schema:addresscountry': 'CA',
                'schema:postalcode': 'J5Y4A9'
            },
            'schema:contactpoint': [
                {
                    '@type': 'schema:contactpoint',
                    'schema:email': 'test@test.com'
                },
                {
                    '@type': 'schema:contactpoint',
                    'schema:email': 'test2@test2.com'
                }
            ]
    }
    ref_record = { 
            '@type': 'schema:test',
            '@id': 'test_id',
            'schema:name': 'Test record',
            'schema:url': 'https://www.test.com',
            'schema:address': {
                '@type': 'schema:postaladdress',
                '@id': 'addr1'
            },
            'schema:contactpoint': [
                {
                    '@type': 'schema:contactpoint',
                    '@id': 'test@test.com'
                },
                {
                    '@type': 'schema:contactpoint',
                    '@id': 'test2@test2.com'
                }
            ]
    }


    s = Schema()


    main_record, list = s.get_main_record(input_record)

    assert(main_record) == ref_record