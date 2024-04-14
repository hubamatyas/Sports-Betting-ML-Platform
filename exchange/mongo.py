import gridfs
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
from pymongo.database import Database
from gridfs.errors import FileExists
from pymongo.errors import DuplicateKeyError
from enums import MongoURIs
from enum import Enum
import concurrent.futures
import multiprocessing


class MongoDB():
    def __init__(self, db: Enum, uri: Enum = MongoURIs.Test):
        """
        Initializes a MongoDB client connection using a specified database and URI.

        Parameters:
        - db (Enum): The database Enum to connect to.
        - uri (Enum): The MongoDB URI Enum to use for the connection. Defaults to MongoURIs.Test.
        """

        self.uri = uri.value
        self.client = MongoClient(self.uri, server_api=ServerApi('1'), maxPoolSize=1000)
        self.db = self.client[db.value]


    def insert_one(self, collection: Enum, data):
        """
        Inserts a single document into a specified MongoDB collection. The database must already
        be set inside `self.db`.

        Parameters:
        - collection (Enum): The collection Enum where the document will be inserted.
        - data (dict): The document data to insert.

        Returns:
        - InsertOneResult: The result of the insert operation, including the inserted document's ID.
        """

        collection: Collection = self.db[collection.value]

        try:
            result = collection.insert_one(data)
            print(f"Inserted document with id '{result.inserted_id}' into the '{collection.name}'.")
        except DuplicateKeyError as e:
            print(f"Duplicate Error: Failed to upload with error into '{collection.name}': {e}")
            result = e

        return result


    def insert_many(self, collection: Enum, data: list, metadata_duplicated = None, type: str = None):
        """
        Load multiple files (ie. data) to a MongoDB Collection. Specify metadata_duplicated
        if you only want to upload ladder data if its associated metadata is not duplicated.
        Time series doesn't support unique indexes. A DuplicateKeyError would never be raised
        when uploading ladder data, therefore, we need to check if the metadata is duplicated.
        You might still want to upload ladder data even if the metadata is duplicated, in this
        case, don't specify metadata_duplicated when calling insert many. Function implements
        multiprocessing to increase efficiency when uploading more than 1000 documents.

        Parameters:
        - collection (Enum): The collection Enum where the documents will be inserted.
        - data (list): A list of documents to insert.
        - metadata_duplicated: An optional parameter to specify handling of duplicated metadata.
        - type (str): An optional parameter to specify if the collection is a time series. Use "ts" for time series.

        Returns:
        - InsertManyResult: The result of the insert operation, including the IDs of the inserted documents.
        """

        if isinstance(metadata_duplicated, DuplicateKeyError):
            print(f"Duplicate Error: Not uploading data files to '{collection.name}' because metadata is duplicated.")
            return
        
        # Create time series collection with specific options if it doesn't exist
        if collection.value not in self.db.list_collection_names() and type == "ts":
            timeseries = {
                "timeField": "pt",
                "metaField": "metadata",
                "granularity": "seconds"
            }

            self.db.create_collection(
                collection.value,
                timeseries=timeseries
            )

        collection: Collection = self.db[collection.value]
        try:
            if len(data) < 1000:
                # Insert directly if data is small
                result = collection.insert_many(data, ordered=False)
            else:
                # Insert in batches of 1000 with multithreading
                with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                    for i in range(0, len(data), 1000):
                        future = executor.submit(collection.insert_many, data[i:i+1000], ordered=False)

                    result = future.result()

            print(f"Inserted '{len(data)}' documents into the '{collection.name}'.")

        except DuplicateKeyError as e:
            result = e
            print(f"Duplicate Error: Failed to upload with error into '{collection.name}': {e}")
        
        return result
    

    def find(self, collection: Enum, filter: dict, projection: dict = None):
        """
        Execute the query to get the list of matching documents in the specified collection.
        The database must already be set inside `self.db`.

        Parameters:
        - collection (Enum): The collection Enum to query.
        - filter (dict): The query filter.
        - projection (dict): An optional projection specifying which fields to include or exclude.

        Returns:
        - list: A list of queried documents.
        """

        collection: Collection = self.db[collection.value]
        mongoCursor = collection.find(filter, projection)
        return list(mongoCursor)
    

    def find_one(self, collection: Enum, filter: dict, projection: dict = None):
        """
        Execute the query to get one matching documents in the specified collection.
        The database must already be set inside `self.db`.

        Parameters:
        - collection (Enum): The collection Enum to query.
        - filter (dict): The query filter.
        - projection (dict): An optional projection specifying which fields to include or exclude.

        Returns:
        - dict: The one queried documents.
        """

        collection: Collection = self.db[collection.value]
        return collection.find_one(filter, projection)


class GridFs():
    """
    Handle file uploads and retrievals from GridFS.
    """

    def __init__(self, db: Database, prefix: Enum):
        """
        Initializes a GridFS instance for file storage in MongoDB.

        Parameters:
        - db (Database): The MongoDB database object.
        - prefix (Enum): The prefix for the GridFS collections (files and chunks).
        """
         
        self.db = db
        self.prefix = prefix.value
        self.fs = gridfs.GridFS(self.db, self.prefix)
        

    def upload_file(self, json_content, metadata):
        """
        Uploads a file to GridFS with specified content and metadata.

        Parameters:
        - json_content (dict): The JSON content of the file to upload.
        - metadata (dict): The metadata associated with the file, including 'marketId' and 'eventId'.

        Returns:
        - ObjectId: The ID of the inserted file in GridFS.
        """

        market_id = metadata["id"]
        event_id = metadata["marketDefinition"]["eventId"]

        # eg. "ladders_1.208050898_31954040"
        id = f"{self.prefix}_{market_id}_{event_id}"
        data_str = json.dumps(json_content)

        file_metadata = {
            "eventId": event_id,
            "marketId": market_id,
            "_id": id,
        }

        try:
            reponse = self.fs.put(data_str, filename=market_id, encoding='utf-8', **file_metadata)
            print(f"Inserted document with id '{id}' into the '{self.prefix}' GridFS.")
            return reponse
        except FileExists as e:
            print(f"Duplicate Error: Failed to upload '{id}' into '{self.prefix}' GridFS with error: {e}")


    def retrieve_file_from_gridfs(self, file_metadata):
        """
        Retrieve a file from GridFS using the specified file metadata.

        Parameters:
        - file_metadata (dict): Metadata used to identify the file, including 'marketId' and 'eventId'.

        Returns:
        - dict: The content of the retrieved file as a JSON dictionary.
        """

        file_document = self.fs.find(file_metadata)
        documents = list(file_document)
        if len(documents) == 0:
            print("No documents found with the specified metadata.")
            return
        
        print("Documents found:")
        for doc in documents:
            print("File Document:", doc)
            file_id = doc._id
            chunks_query = {"files_id": file_id}
            chunks_collection_name = self.fs._GridFS__collection.name + ".chunks"
            chunks_cursor = self.db[chunks_collection_name].find(chunks_query).sort("n")

            # Collect binary data from chunks
            file_data = b''
            for chunk in chunks_cursor:
                file_data += chunk['data']

            # Decode binary data to string
            json_data_str = file_data.decode('utf-8')
            json_data = json.loads(json_data_str)

        return json_data
    

    def get_grid_fs_id(self, metadata):
        """
        Constructs a dictionary representing the GridFS ID based on given metadata.

        Parameters:
        - metadata (dict): Metadata containing 'marketId' and 'eventId'.

        Returns:
        - dict: A dictionary suitable for use as a GridFS file identifier.
        """

        market_id = metadata["marketId"]
        event_id = metadata["marketDefinition"]["eventId"]

        # dict must match the schema in GridFsHandler.upload_file
        return {"eventId": event_id, "marketId": market_id}
