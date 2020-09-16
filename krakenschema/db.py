
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from flatten_dict import flatten



class Db:

    def __init__(self):
        # Initialize database
        project_id = 'kraken-v1'

        if (not len(firebase_admin._apps)):

            try:
                # Initialize from  google
                cred = credentials.Certificate("key.json")
                firebase_admin.initialize_app(cred, {
                    'projectId': project_id,
                })
                self.db = firestore.client()
            except:

                cred = credentials.ApplicationDefault()
                firebase_admin.initialize_app(cred, {
                    'projectId': project_id,
                })
                self.db = firestore.client()
        else:
            self.db = firestore.client()


    def get(self, doc_path):
        

        query = self.db.document(doc_path)

        # Find document by document name
        doc = query.get()

        # Convert to dict
        record = doc.to_dict()

        return record


    def post(self, doc_path, record):
        
        doc = self.db.document(doc_path)

        # Find document by document name
        doc.set(record)

        # Convert to dict
        record_id = doc.id
        record_path = doc.path

        return record

    def update(self, doc_path, record):
        

        flat_record = flatten(record, reducer='dot', enumerate_types=(list,))

        new_flat = {}
        for i in flat_record:

            fpath = i.split('.')
            if len(fpath) == 1:
                path = self.db.field_path(fpath[0])
            elif len(fpath) == 2:
                path = self.db.field_path(fpath[0], fpath[1])
            elif len(fpath) == 3:
                path = self.db.field_path(fpath[0], fpath[1], fpath[2])
            elif len(fpath) == 4:
                path = self.db.field_path(fpath[0], fpath[1], fpath[2], fpath[3])
            
            new_flat[path] = flat_record[i]

        doc = self.db.document(doc_path)

        # Find document by document name
        doc.update(new_flat)

        # Convert to dict
        record_id = doc.id
        record_path = doc.path

        return record
