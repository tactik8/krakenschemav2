
from krakenschema.cache import Cache
from krakenschema.schema import Schema

import inspect


class Tests_cache:
    def test_post(self):
        cache = Cache()

        schema = Schema()
        input_record = schema.get_test()
        input_record_type = input_record.get('@type', None)
        input_record_id = input_record.get('@id', None)

        cache.post(input_record_type, input_record_id, input_record, None)
        output_record, metadata = cache.get(input_record_type, input_record_id)

        fn_name = inspect.stack()[0][3]

        if input_record == output_record:
            print(fn_name, 'pass')
        else: 
            print(fn_name, 'fail')


            
    def test_update(self):
        cache = Cache()
        record_type = 'schema:test'
        record_id = 'test_post_02'

        path = 'schema:test/test_post_02'
        
        record_original = {
                '@type': 'schema:test',
                '@id': 'test_post_02',
                'schema:name': 'test post 02',
                'schema:url': 'https://www.test.com',
                'schema:email': 'sd@sd.com'
            }
        metadata_original= {
            'schema:name': {
                'kraken:credibility': 50,
                'kraken:id': 'test id'
                }
            }

        record_delta = {
                '@type': 'schema:test',
                '@id': 'test_post_02',
                'schema:name': 'test post TEST',
                'schema:test': 'test'
            }
        
        metadata_delta= {
            'schema:name': {
                'kraken:credibility': 60,
                'kraken:id': 'test id'
                }
            }
        

        record_reference = {
                '@type': 'schema:test',
                '@id': 'test_post_02',
                'schema:name': 'test post TEST',
                'schema:test': 'test',
                'schema:url': 'https://www.test.com',
                'schema:email': 'sd@sd.com'
            }

        metadata_reference= {
            'schema:name': {
                'kraken:credibility': 60,
                'kraken:id': 'test id'
            }
        }


        cache.post(record_type, record_id, record_original, metadata_original)
        cache.update(record_type, record_id, record_delta, metadata_delta)

        new_record, new_metadata = cache.get(record_type, record_id)
        
        fn_name = inspect.stack()[0][3]

        if record_reference == new_record and metadata_reference == new_metadata:
            print(fn_name, 'pass')
        else: 
            fn_name = inspect.stack()[0][3]
            print('---')
            print(fn_name, 'fail')
            print(new_metadata)
            print(metadata_reference)
            print('---')