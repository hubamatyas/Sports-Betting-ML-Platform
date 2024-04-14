from mongo import MongoDB
from enums import MongoURIs, Databases, Collections
from query_utils import MarketType

mongo_client = MongoDB(Databases.HorseRacing, MongoURIs.Test)
metadata_files = mongo_client.find(
    collection = Collections.Metadata,
    filter = {"marketDefinition.marketType": MarketType.WIN.value},
    # projection = {"id": 1}
)

qualifying_count = 0
matching_count = 0

# Query directly into the individual ladder updates
for metadata in metadata_files:
    # Find random runner id
    random_runner = str(metadata["marketDefinition"]["runners"][0]["id"])

    # Get in-play start time from metadata
    in_play_start_time = metadata["inPlayStartTime"]

    # If race went in-play then it's a qualifying market
    # Note, there are edge caes where the market never goes in-play
    if in_play_start_time:
        qualifying_count += 1

    # Get the best back price and size 5 minutes before in-play for the random runner
    best_back_5_minutes_before_in_play_price = metadata["pre5ladder"]["runners"][random_runner]["atb"][0][0]
    best_back_5_minutes_before_in_play_size = metadata["pre5ladder"]["runners"][random_runner]["atb"][0][1]

    # Set price threshold
    price_threshold = best_back_5_minutes_before_in_play_price * 0.9

    # Query for ladders that match the price threshold for the random runner
    filtered_ladders = mongo_client.find_one(
        collection = Collections.Ladders,
        filter = {
            # Match the market id
            "marketId": metadata["id"],
            # Only query in-play ladders
            "pt": {"$gt": in_play_start_time},
            # Filter the best back price for the random runner
            f"runners.{random_runner}.atb.0.0": {"$lt": price_threshold},
        },
    )

    if filtered_ladders:
        matching_count += 1

print(f"Matched {matching_count} out of {qualifying_count} qualifying markets. Total markets: {len(metadata_files)}")