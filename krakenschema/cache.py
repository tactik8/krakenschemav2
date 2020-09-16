
from krakenhelper.helper import Date


class Cache:
    def __init__(self):
        self.cache = {}
        a=1

    def get(self, record_type, record_id):

        if not self.cache.get(record_type, None):
            return None

        if not self.cache[record_type].get(record_id, None):
            return None

        record = self.cache[record_type][record_id].get('record', None)
        metadata = self.cache[record_type][record_id].get('metadata', None)

        return record, metadata


    def post(self, record_type, record_id, record, metadata):
        """ 
        Put / overwrite a record in cache removing current key/value 
        
        Parameters
        ----------
        record_type (str): The record type
        record_id (str): The record id
        record (dict): The record 
        metadata (dict): The metadata

        Returns
        -------
        str : Success  
        """
        
        if not self.cache.get(record_type, None):
            self.cache[record_type] = {}

        if not self.cache[record_type].get(record_id, None):
            self.cache[record_type][record_id] = {}

        
        self.cache[record_type][record_id]['record'] = record
        self.cache[record_type][record_id]['metadata'] = metadata

        
        d = Date()
        self.cache[record_type][record_id]['last_updated'] = d.now()

        # Check space, remove old items if not enough space


    def update(self, record_type, record_id, record, metadata):
        """ 
        Update a record in cache without removing current key/value if not in new record (delta)
        
        Parameters
        ----------
        record_type (str): The record type
        record_id (str): The record id
        record (dict): The record 
        metadata (dict): The metadata

        Returns
        -------
        str : Success  
        """
        
        # reate dict keys if doesn't exist
        if not self.cache.get(record_type, None):
            self.cache[record_type] = {}

        if not self.cache[record_type].get(record_id, None):
            self.cache[record_type][record_id] = {}
        
        # Assign new key/values
        for key in record:
            if key in ['@type', '@id']:
                continue
            if record.get(key, None):
                self.cache[record_type][record_id]['record'][key] = record.get(key, None)
            if metadata.get(key, None):
                self.cache[record_type][record_id]['metadata'][key] = metadata.get(key, None)




    def search(self, record_type, key, value):

        # Error handling
        if not key or not value:
            return None

        if not self.cache.get(record_type, None):
            return None

        # Iterates through records and return id of one that fits
        for i in self.cache.get(record_type, []):
            if self.cache[record_type][i]['record'][key] == value:
                return i

        return None