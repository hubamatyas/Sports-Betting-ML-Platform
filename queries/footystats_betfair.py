from interface.mongo import MongoDB
from exchange.enums import MongoURIs, Databases, Collections
from interface.mongo import MongoDB
from sqlalchemy import create_engine
import pandas as pd
from query_utils import FootystatsToBetfair
from query_utils import MarketType
import json


def create_sql_db_engine():
    db_username = 'your_username'
    db_password = 'your_password'
    db_host = 'your_host'
    db_port = 'your_port'
    db_name = 'your_db_name'
    return create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')


def get_sql_query():
    return \
        """
        select *, FROM_UNIXTIME(date_unix) as date_time
        from pre_football pf 
        where
            season = '2022/2023'
            and home_name = 'Tottenham Hotspur'
            and awayGoalCount = 2
            and homeGoalCount  = 0
            and FROM_UNIXTIME(date_unix) < '2023-01-02' 
        """


def execute_sql_query(query, sql_connection):
    # If function throws it's most likely due to wrong sqlalchemy version
    # run pip install --force-reinstall 'sqlalchemy<2.0.0' to fix error
    return pd.read_sql(query, con=sql_connection)


def get_mongo_query(df):
    database_joiner = FootystatsToBetfair(
        home_team = df["home_name"].iloc[0],
        away_team = df["away_name"].iloc[0],
        timestamp = df["date_time"].iloc[0],
        market_type = MarketType.MATCH_ODDS
    )

    return database_joiner.create_mongo_query()



if __name__ == "__main__":
    mongo_client = MongoDB(Databases.Football, MongoURIs.Serverless)
    
    sql_connection = create_sql_db_engine()
    sql_query = get_sql_query()
    df = execute_sql_query(sql_query, sql_connection)

    query = get_mongo_query(df)
    metadata_files = mongo_client.find(
        collection = Collections.Metadata,
        filter = query,
    )

    sample_metadata = metadata_files[0]
    print(json.dumps(sample_metadata, indent=4, default=str))
