import requests
import json
from krakenschema.schema import Schema


print(' -------- schema --------')

s = Schema()

test_record = s.get_test()
a, b = s.get_main_record(test_record)
print(a)

