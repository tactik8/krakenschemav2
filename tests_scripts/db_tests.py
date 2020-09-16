from krakenschema.db import Db
import inspect



class Tests_db:

    def __init__(self):
        a=1

    def test_post(self):
        db = Db()

        path = 'schema:test/test_post_01'
        record = {
            'data': {
                '@type': 'schema:test',
                '@id': 'test_post_01',
                'schema:name': 'test post 01'
            },
            'metadata': {
                'schema:name': {
                    'kraken:credibility': 50,
                    'kraken:id': 'test id'
                }
            }
        }
        db.post(path, record)

        new_record = db.get(path)
        
        fn_name = inspect.stack()[0][3]

        if record == new_record:
            print(fn_name, 'pass')
        else: 
            print(fn_name, 'fail')

        
    def test_update(self):
        db = Db()

        path = 'schema:test/test_post_02'
        record_original = {
            'data': {
                '@type': 'schema:test',
                '@id': 'test_post_02',
                'schema:name': 'test post 02',
                'schema:url': 'https://www.test.com',
                'schema:email': 'sd@sd.com'
            },
            'metadata': {
                'schema:name': {
                    'kraken:credibility': 50,
                    'kraken:id': 'test id'
                }
            }
        }

        record_delta = {
            'data': {
                '@type': 'schema:test',
                '@id': 'test_post_02',
                'schema:name': 'test post TEST',
                'schema:test': 'test'
            },
            'metadata': {
                'schema:name': {
                    'kraken:credibility': 60,
                    'kraken:id': 'test id'
                }
            }
        }

        record_reference = {
            'data': {
                '@type': 'schema:test',
                '@id': 'test_post_02',
                'schema:name': 'test post TEST',
                'schema:test': 'test',
                'schema:url': 'https://www.test.com',
                'schema:email': 'sd@sd.com'
            },
            'metadata': {
                'schema:name': {
                    'kraken:credibility': 60,
                    'kraken:id': 'test id'
                }
            }
        }


        db.post(path, record_original)
        db.update(path, record_delta)

        new_record = db.get(path)
        
        fn_name = inspect.stack()[0][3]

        if record_reference == new_record:
            print(fn_name, 'pass')
        else: 
            print(fn_name, 'fail')
            print(new_record)