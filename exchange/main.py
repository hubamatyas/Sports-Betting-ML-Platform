from ladder import LadderBuilder
from s3 import S3
from mongo import MongoDB, GridFs
import betfair_utils
from enums import MarketFilters, Sport, CountryFilters, Collections, Databases, MongoURIs, MetaBuilder
import json
import time
import concurrent.futures
import multiprocessing
import threading


def run(s3: S3, mongodb: MongoDB, grid_fs: GridFs, file_key: str, market_filter: MarketFilters, meta_builder: MetaBuilder, country_filter: CountryFilters):
    """
    Run the pipeline for a single file retrieved from the specified folder in S3.
    1. Check if the file is a market file and decompress it (from bz2 to string list)
    2. JSON Load the market updates and extract the market definition from the market updates
    3. Build ladders if market matches the market and country filters
    4. Upload the marketdata to MongoDB (GridFS), Upload the metadata to MongoDB,
    upload the ladders to MongoDB (TimeSeries) and upload the raw marketdata to MongoDB (TimeSeries)
    """

    if not betfair_utils.is_market_file(file_key): return
    market_string_updates: list[str] = s3.get_file_content(file_key)
    if not market_string_updates: return

    marketdata: list[dict] = betfair_utils.json_load_updates(market_string_updates)
    market_definition: dict = betfair_utils.get_market_definition(marketdata)

    if not betfair_utils.is_matching_filters(market_definition, market_filter, country_filter): return
    metadata, ladders, ts_marketdata = LadderBuilder(marketdata, market_definition, meta_builder).run()

    # result_market = grid_fs.upload_file(marketdata, metadata)
    result_meta = mongodb.insert_one(Collections.Metadata, metadata)
    result_ladders = mongodb.insert_many(Collections.Ladders, ladders, result_meta, type="ts")
    result_ts_marketdata = mongodb.insert_many(Collections.Marketdata, ts_marketdata, result_meta, type="ts")

    print(f"Finished handling '{file_key} with thread id: {threading.current_thread().ident}'\n")


def main(folder: str, uri: MongoURIs, market_filter: MarketFilters, meta_builder: MetaBuilder, country_filter: CountryFilters, database: Databases, is_multiprocess: bool):
    s3_interface = S3(folder)
    mongo_interface = MongoDB(database, uri)
    grid_fs_interface = GridFs(mongo_interface.db, Collections.Marketdata)

    if not is_multiprocess:
        for file_key in s3_interface.fetch_files_from_s3():
            run(
                s3=s3_interface,
                mongodb=mongo_interface,
                grid_fs=grid_fs_interface,
                file_key=file_key,
                market_filter=market_filter,
                meta_builder=meta_builder,
                country_filter=country_filter
            )
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            for file_key in s3_interface.fetch_files_from_s3():
                future = executor.submit(
                    run,
                    s3=s3_interface,
                    mongodb=mongo_interface,
                    grid_fs=grid_fs_interface,
                    file_key=file_key,
                    market_filter=market_filter,
                    meta_builder=meta_builder,
                    country_filter=country_filter
                )

            result = future.result()
    

if __name__ == "__main__":
    # Specify these parameters manually
    # ==========================================================
    folder = 'research-repo/betfair-data/horse-racing/2023/Jan/3'
    sport = Sport.HorseRacing
    uri = MongoURIs.Serverless
    is_multiprocess = True
    # ==========================================================

    if sport == Sport.HorseRacing:
        main(
            folder,
            uri,
            MarketFilters.HorseRacingMarketRegex,
            MetaBuilder.HorseRacing,
            CountryFilters.HorseRacingCountryRegex,
            Databases.HorseRacing,
            is_multiprocess
        )
    elif sport == Sport.Football:
        main(
            folder,
            uri,
            MarketFilters.FootballMarketRegex,
            MetaBuilder.Football,
            CountryFilters.FootballCountryRegex,
            Databases.Football,
            is_multiprocess
        )
    elif sport == Sport.Tennis:
        main(
            folder,
            uri,
            MarketFilters.TennisMarketRegex,
            MetaBuilder.Tennis,
            CountryFilters.TennisCountryRegex,
            Databases.Tennis,
            is_multiprocess
        )