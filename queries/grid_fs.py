from interface.mongo import MongoDB, GridFs
from exchange.enums import MongoURIs, Databases, Collections
from query_utils import MarketType
import json


mongo_client = MongoDB(Databases.Football, MongoURIs.Test)
metadata_files = mongo_client.find(
    collection = Collections.Metadata,
    filter = {"marketDefinition.marketType": MarketType.OVER_UNDER_25.value},
)

gridfs_client = GridFs(mongo_client.db, Collections.Marketdata)

sample_metadata = metadata_files[0]
gridfs_id = gridfs_client.get_grid_fs_id(sample_metadata)
sample_marketdata: dict = gridfs_client.retrieve_file_from_gridfs(gridfs_id)

# Print sample update from raw market data
print(json.dumps(sample_marketdata["marketData"][111], indent=4))