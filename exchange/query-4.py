from mongo import MongoDB
from enums import MongoURIs, Databases, Collections
from query_utils import MarketType
from bson.son import SON  # For ordered dictionary in projections

mongo_client = MongoDB(Databases.HorseRacing, MongoURIs.Serverless)
ladder_collection = mongo_client.db[Collections.Ladders.value]
metadata_collection = mongo_client.db[Collections.Metadata.value]

"""
MongoDB can handle aggregation pipelines that are essentially a sequence of data processing operations (or query stages).
The pipeline below tries to replicate the query in query-1.py, but in a single aggregation pipeline, without the need for
storing intermediate results locally and iterating over them. Currently, the pipeline is broken, but at least shows the
kind of operations and stages it can perform.
"""


pipeline = [
    # Stage 1: Filter for relevant markets
    {
        "$match": {
            "marketDefinition.marketType": "WIN", 
            "inPlayStartTime": { "$exists": True } 
        }
    },

    # Stage 2: Unwind runners array
    {
        "$unwind": "$marketDefinition.runners"
    },

    # Stage 3: Project necessary data, pre-calculate values
    {
        "$project": {
            "_id": 0,  
            "marketId": "$marketId",
            "inPlayStartTime": "$inPlayStartTime",
            "randomRunnerId": "$marketDefinition.runners.id",
            "bestBack5Price": { 
                "$arrayElemAt": [ "$pre5ladder.runners.$marketDefinition.runners.id.atb", 0] 
            }, 
            "priceThreshold": { 
                "$multiply": [ 
                    { "$arrayElemAt": [ "$pre5ladder.runners.$marketDefinition.runners.id.atb", 0] }, 
                    0.9 
                ] 
            } 
        }
    },

    # Stage 4: Lookup matching ladders
    {
        "$lookup": {
            "from": "ladders",
            "let": { 
                "marketId": "$marketId", 
                "inPlayStartTime": "$inPlayStartTime", 
                "runnerId": "$randomRunnerId",
                "priceThreshold": "$priceThreshold"
            },
            "pipeline": [
                { 
                    "$match": {
                        "$expr": {
                            "$and": [
                                { "$eq": [ "$metadata", "$$marketId" ] },
                                { "$gt": [ "$pt", "$$inPlayStartTime" ] },
                                { "$lt": [ "$runners.runnerId.atb.0.0", "$$priceThreshold" ] }
                            ]
                        }
                    }
                }
            ],
            "as": "matchingLadders"
        }
    },

    # Stage 5: Check if matching ladders were found
    {
        "$match": { "matchingLadders": { "$ne": [] } } 
    },

    # Stage 6: Count the final results
    {
        "$group": {
            "_id": None, 
            "qualifyingCount": { "$sum": 1 }, 
            "matchingCount": { "$sum": 1 }
        }
    }
]

result = metadata_collection.aggregate(pipeline)

# Print the results (assuming the aggregation yields a single document)
for doc in result:
    print(f"Matched {doc['matchingCount']} out of {doc['qualifyingCount']} qualifying markets.") 
