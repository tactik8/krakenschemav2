import re

from krakenhelper.helper import Url, Date, UUID
from krakenschema.cache import Cache
from krakenschema.db import Db

cache = Cache()
db = Db()


class Schema:
    def __init__(self, record_type=None, record_id=None, record=None):
        self.record_type = record_type
        self.record_id = record_id
        self.record = record
        self.metadata = None

        # Reference record from db
        self.ref_record = None
        self.ref_metadata = None

        # Delta with ref record
        self.delta_record = None
        self.delta_metadata = None

        self.main_record = None
        self.record_list = None

        self.valid = None
        self.ref_id = None

        if self.record:
            self._get_record_type()
            self._get_record_id()

    def _get_record_type(self):

        if not self.record_type:
            if self.record:
                if isinstance(self.record, dict):
                    self.record_type = self.record.get('@type', None)

        self.record['@type'] = self.record_type
        return self.record_type

    def _get_record_id(self):

        if not self.record_id:
            if self.record:
                if isinstance(self.record, dict):
                    self.record_id = self.record.get('@id', None)

        # Assign from record_type if exist
        if not self.record_id:

            if self.record_type == 'schema:website':
                self.record_id = self.get_url_id()

            elif self.record_type == 'schema:webpage':
                self.record_id = self.get_url_id()

            elif self.record_type == 'schema:videoobject':
                self.record_id = self.get_contenturl_id()

            elif self.record_type == 'schema:imageobject':
                self.record_id = self.get_contenturl_id()

            elif self.record_type == 'schema:person':
                self.record_id = self.get_email()

            elif self.record_type == 'schema:contactpoint':
                self.record_id = self.get_email()

            elif self.record_type == 'schema:organization':
                self.record_id = self.get_domain()

            elif self.record_type == 'schema:action':
                u = UUID()
                self.record_id = u.get()

            elif self.record_type == 'schema:message':
                u = UUID()
                self.record_id = u.get()

        self.record['@id'] = self.record_id

        return self.record_id

    def get(self):

        # Get from cache
        self.record, self.metadata = cache.get(self.record_type,
                                               self.record_id)

        if not self.record:
            # Get from db
            path = self.record_type + '/' + self.record_id
            db_record = db.get(path)
            self.record = db_record.get('data')
            self.metadata = db_record.get('metadata')

        return self.record

    def post(self, record=None):
        """ 
        Post record and sub records in database

        Algorithm flatten record, producing a list of records. It then tries to set a record_id, find one and fnially set uuid as record_id if it cannot find one. It then calculates the delta between the new record and what is currently in the db. Finally, iit writes all new records at once. 

        Parameters
        ----------
        self (class): The record  

        Returns
        -------

        """

        if record:
            self.record = record

        # Flatten record, producing a list with all sub records
        record, record_list = self.get_main_record()

        # Convert to schema_list
        new_record_list = []
        for i in record_list:
            schema = Schema(None, None, i)
            new_record_list.append(schema)
        record_list = new_record_list

        # Assign record id for list of record_list
        key_map = {}
        for i in record_list:

            sub_record = Schema()
            original_record_id = i.record_id

            # Assign record_id
            i._get_record_id(i)

            # Search records if not found
            if not i.record_id:
                i.search_record_id()

            # Add in ref dict if starts with temp
            if original_record_id.startswith('temp'):
                key_map[original_record_id] = i.record_id

        # Cycle through records to replace temp id
        for i in record_list:
            for old_value in key_map:
                key = '@id'
                new_value = key_map[old_value]
                i.replace_value(i, key, old_value, new_value)

        # Retrieve delta record
        for i in record_list:
            i._get_delta()

        # Save records
        for i in record_list:
            i._post_delta()

        return self.record_id

    def _post_delta(self):
        """ 
        Post the delta record to db.

        Parameters
        ----------

        Returns
        -------
        itself

        """

        # Verify if delta empty
        if not self.delta_record:
            return

        # Post datapoint
        u = UUID()
        path = self.record_type + '/' + self.record_id + '/datapoints/' + u.get(
        )

        d = Date()

        db_record = {
            'data': self.delta_record,
            'metadata': self.delta_metadata,
            'updated_date': d.now()
        }

        db.update(path, db_record)

        # Post update to main record
        path = self.record_type + '/' + self.record_id

        db_record = {
            'data': self.delta_record,
            'metadata': self.delta_metadata
        }

        db.update(path, db_record)

        # Update cache
        cache.update(self.record_type, self.record_id, self.delta_record,
                     self.delta_metadata)

    def _get_delta(self):
        """ 
        Compare two schema and return the delta with the best criteria (credibility, date, etc)

        Parameters
        ----------
        self (class): The record challenging 
        ref_record (class): The record currently best

        Returns
        -------
        class : A new class object with the delta  
        """

        # Error handling
        if not self.record:
            self.record = {}
        if not self.ref_record:
            self.ref_record = {}
        if not self.delta_record:
            self.delta_record = {}

        if not self.metadata:
            self.metadata = {}
        if not self.ref_metadata:
            self.ref_metadata = {}
        if not self.delta_metadata:
            self.delta_metadata = {}

        # Iterate keys
        for key in self.record:
            value1 = self.record.get(key, None)
            metadata1 = self.metadata.get(key, {})

            value2 = self.ref_record.get(key, {})
            metadata2 = self.ref_metadata.get(key, {})

            d_record, d_metadata = self._get_diff_key(value1, metadata1,
                                                      value2, metadata2)

            # Assign if not empty
            if d_record:
                self.delta_record[key] = d_record
            if d_metadata:
                self.delta_metadata[key] = d_metadata

        return

    def _get_ref(self):
        """ 
        Retrieves a reference schema from database

        Parameters
        ----------
        self (class): The record challenging 

        Returns
        -------
        class : A new class object with the delta  
        """

        self.ref_record = {}
        self.ref_metadata = {}

        # Check in cache
        self.ref_record, self.ref_metadata = cache.get(self.record_type,
                                                       self.record_id)

        if not self.ref_record:
            # Check in db
            record = {}

    def _get_diff_key(self, value1, metadata1, value2, metadata2):

        # Handle empty values
        if not value1 and not value2:
            return None, None
        if not value2:
            return value1, metadata1
        if not value1:
            return value2, metadata2

        if value1 == value2:
            return None, None

        # Iterate through critera
        criteria = ['kraken:credibility', 'kraken:created_date']
        for c in criteria:
            c1 = metadata1.get(c, None)
            c2 = metadata2.get(c, None)

            if not c1:
                continue
            elif not c2:
                return value1, metadata1
            elif c1 == c2:
                continue
            elif c1 > c2:
                return value1, metadata1
            elif c1 < c2:
                return value2, metadata2

        # Finally return new record since it is probably more recent
        return value1, metadata1

    def replace_value(self, record, key, old_value, new_value):

        self.record = record

        def replace_value(record, key, old_value, new_value):
            if isinstance(record, str):
                new_record = record

            elif isinstance(record, dict):
                new_record = {}
                for k in record:
                    new_record[k] = replace_value(record[k], key, old_value,
                                                  new_value)

                if new_record.get(key, None) == old_value:
                    new_record[key] = new_value

            elif isinstance(record, list):
                new_record = []
                for i in record:
                    new_record.append(
                        replace_value(i, key, old_value, new_value))
            else:
                new_record = record

            return new_record

        self.record = replace_value(self.record, key, old_value, new_value)
        return self.record

    def pre_processing(self):

        if not self.record_id:
            self.search_record_id()

        if not self.record_id:
            u = UUID()
            self.record_id = u.get()

    def get_valid(self, record=None):
        """
        Check if schema is valid

        Parameters
        ----------
        record (dict): The record to check

        Returns
        -------
        bool : True if valid schema
        """

        if record:
            self.record = record

        self._get_record_type()

        if self.record_type:
            self.valid = True
        else:
            self.valid = False

        return self.valid

    def get_ref_id(self, record=None):
        """
        Get the ref_id of a record, the shorthand notation

        Parameters
        ----------
        record (dict): The record

        Returns
        -------
        dict : A ref_id dict or None if invalid
        """

        if record:
            self.record = record

        # Process record
        self._get_record_type()
        self._get_record_id()

        # Error handling
        if self.get_valid() == False:
            return None

        if not self.record_type or not self.record_id:
            return None

        # Get ref_id
        self.ref_id = {'@type': self.record_type, '@id': self.record_id}

        return self.ref_id

    def search_record_id(self):
        # Search record in database if exist, returns record-id if so

        return self.record_id

    def get_main_record(self, record=None):
        """
        Repleaces sub_records with their ref_id reference, returning a simpler record

        Parameters
        ----------
        record (dict): The record

        Returns
        -------
        dict : A simplified record with ref_if for sub records
        """

        if record:
            self.record = record

        # Error handling
        if not self.record or not isinstance(self.record, dict):
            return None

        # Initialize record list
        self.record_list = []

        def _process_dict(record, parent):

            if not record.get('@type', None):
                return record

            # Assign itself as parent
            schema = Schema()
            record_ref_id = schema.get_ref_id(record)

            # Assign temp id if no record_id
            if not schema.record_id:
                u = UUID()
                schema.record_id = 'temp' + u.get()
                record_ref_id = schema.get_ref_id()

            # Iterate through keys
            new_record = {}
            for key in record:
                new_record[key] = _process_record(record[key], record_ref_id)

            # If parent exist, assign parent to record and return ref_id
            if parent:
                # assign parent record
                parent_schema = Schema()
                new_record['kraken:parent'] = parent_schema.get_ref_id(parent)

                # Add to record list
                self.record_list.append(new_record)

                # Return
                return record_ref_id

            else:
                # Return record
                self.record_list.append(new_record)
                return new_record

        def _process_list(record, parent):
            new_record = []
            for i in record:
                new_record.append(_process_record(i, parent))
            return new_record

        def _process_record(record, parent=None):

            if isinstance(record, str):
                new_record = record

            elif isinstance(record, dict):
                new_record = _process_dict(record, parent)

            elif isinstance(record, list):
                new_record = _process_list(record, parent)

            else:
                new_record = record

            return new_record

        self.main_record = _process_record(self.record)
        return self.main_record, self.record_list

    def flatten_schema(self, record):
        record, record_list = self._process_schema(
            record, keep=False, temp_id=True)

    def _process_schema(self, record, keep=True, temp_id=True):
        # Decompose nested schema record into a list
        # if keep == False, replace sub_record by their ref_id
        # If temp_id == True, repalce mepty @id by a temp one

        # Operation (recursive)
        def _flatten_iterate(record, id_count=0):
            schema_list = []

            if isinstance(record, dict):

                #Set temp id if none
                record_type = record.get('@type', None)
                record_id = record.get('@id', None)

                if record_type and not record_id and temp_id == True:
                    record['@id'] = '_temp_id_' + str(id_count)
                    id_count += 1

                # Iterate through keys (recursion)
                new_record = {}
                for i in record:

                    sub_record, sub_list = _flatten_iterate(
                        record[i], id_count)

                    if sub_list:
                        schema_list += sub_list
                    if self.schema_test_valid(sub_record) == True:
                        if keep == True:
                            new_record[i] = sub_record
                        else:
                            new_record[i] = self.get_ref_id(sub_record)
                    else:
                        new_record[i] = sub_record

                # Check if schema, add if so
                if self.schema_test_valid(new_record) == True:
                    # Add record to list
                    schema_list.append(new_record)

            elif isinstance(record, list) and not isinstance(record, str):
                new_record = []
                for r in record:
                    new_sub, sub_list = _flatten_iterate(r, keep, id_count)

                    if new_sub:
                        new_record.append(new_sub)
                    if sub_list:
                        schema_list += sub_list
            else:
                a = 1  # do nothing
                new_record = record

            return new_record, schema_list

        # Decompose nested schema record into a list

        # Error handling
        # Convert to normal record if list of 1
        if isinstance(record, list) and len(record) == 1:
            record = record[0]
        # If not record
        elif not isinstance(record, dict):
            self.result = None
            self.status = False

        # Process
        record, schema_list = _flatten_iterate(record)

        self.value = record, schema_list
        return self.value

    def get_test(self):
        """
        Returns a generic test schema

        Parameters
        ----------

        Returns
        -------
        dict : A generic test schema
        """

        self.record_type = 'schema:test'
        self.record_id = 'generic id'
        self.record = {
            '@type':
            self.record_type,
            '@id':
            self.record_id,
            'schema:name':
            'Test record',
            'schema:url':
            'https://www.test.com',
            'schema:address': {
                '@type': 'schema:postaladdress',
                'schema:streetaddress': '269 de Carignan',
                'schema:address:locality': 'Repentigny',
                'schema:addresscountry': 'CA',
                'schema:postalcode': 'J5Y4A9'
            },
            'schema:contactpoint': [{
                '@type': 'schema:contactpoint',
                'schema:email': 'test@test.com'
            },
                                    {
                                        '@type': 'schema:contactpoint',
                                        'schema:email': 'test2@test2.com'
                                    }]
        }
        return self.record

    def get_record(self):
        self._get_record_type()
        self._get_record_id()

        return self.record

    def get_url(self):
        return self.record.get('schema:contenturl', None)

    def get_url_id(self):
        ### give web safe url for id purposes

        self.record_id = self.get_url()

        if self.record_id:
            self.record_id = self.record_id.replace("https://", "")
            self.record_id = self.record_id.replace("http://", "")
            self.record_id = self.record_id.replace("www.", "")
            self.record_id = self.record_id.rstrip("/")

            self.record_id = re.sub("[^0-9a-zA-Z]+", "_", self.record_id)

        return self.record_id

    def get_contenturl(self):
        return self.record.get('schema:contenturl', None)

    def get_contenturl_id(self):
        self.record_id = self.get_contenturl()

        if self.record_id:
            self.record_id = self.record_id.replace("https://", "")
            self.record_id = self.record_id.replace("http://", "")
            self.record_id = self.record_id.replace("www.", "")
            self.record_id = self.record_id.rstrip("/")

            self.record_id = re.sub("[^0-9a-zA-Z]+", "_", self.record_id)

        return self.record_id

    def get_domain(self):
        u = Url()
        return u.get_domain(self.get_url())

    def get_email(self):
        return self.record.get('schema:email', None)

    def get_metadata(self):

        if not self.metadata:
            self._generate_metadata()

        return self.metadata

    def _generate_metadata(self):

        d = Date()

        datasource = self.record.get('kraken:datasource', {})

        # Define base metadata

        for i in self.record:
            self.metadata[i]['kraken_created_date'] = self.record.get(
                'kraken:created_date', None)

            self.metadata[i]['kraken_modified_date'] = self.record.get(
                'kraken:modified_date', None)

            self.metadata[i]['datasource_created_date'] = self.record.get(
                'kraken:datasource_created_date', None)

            self.metadata[i]['datasource_modified_date'] = self.record.get(
                'kraken:datasource_modified_date', None)

            self.metadata[i]['credibility'] = self.record.get(
                'kraken:credibility', 0)
