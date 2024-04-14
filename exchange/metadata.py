import pandas as pd
from datetime import datetime
from query_utils import MarketType
import copy
import re

# Milliseconds to minute constants
FIVE_MINUTES = 5 * 60 * 1000
TEN_MINUTES = 10 * 60 * 1000
ONE_SECOND = 1000

class MetadataBuilder:
    """
    A class to build metadata for market data, including pre-market and post-market ladders,
    winner information, and favourite information, based on the provided market data and timing
    for in-play events. These details are added on top of the last marketDefinition in the
    marketdata.

    This base class handles the common functionality for all sports, with subclasses to handle
    sports-specific metadata.

    Attributes:
    - marketdata (list[dict]): A list of market updates (dictionaries) received for a given market.
    - in_play_start (pd.Timestamp): The timestamp when the market goes in-play.
    - in_play_end (pd.Timestamp): The timestamp when the in-play period ends.
    - pre10ladder (dict): The market state 10 minutes before going in-play.
    - pre5ladder (dict): The market state 5 minutes before going in-play.
    - pre0ladder (dict): The market state immediately before going in-play.
    - metadata (dict): The base metadata structure to be extended with market data.
    - market_id (str): The ID of the market being processed.
    - event_id (str): The ID of the event associated with the market.
    """

    def __init__(self, marketdata: list[dict], in_play_start: pd.Timestamp, in_play_end: pd.Timestamp):
        """
        Initializes the MetadataBuilder with the market data and the timing for the market's in-play period.

        Parameters:
        - marketdata (list[dict]): The market data updates for the market.
        - in_play_start (pd.Timestamp): The timestamp when the market goes in-play.
        - in_play_end (pd.Timestamp): The timestamp when the in-play period ends.
        """

        self.marketdata: list[dict] = marketdata
        self.in_play_start: pd.Timestamp = in_play_start
        self.in_play_end: pd.Timestamp = in_play_end

        self.pre10ladder: dict = None
        self.pre5ladder: dict = None
        self.pre0ladder: dict = None

        self.metadata, self.market_id, self.event_id = self.format_metadata(self.marketdata[-1]["mc"][0])
        self.metadata["inPlayStartTime"] = self.in_play_start
        self.metadata["inPlayEndTime"] = self.in_play_end
        

    @staticmethod
    def format_metadata(mc: dict) -> tuple:
        """
        Takes a market change (ie. Betfair update) and returns the metadata base.
        The metadata base contains the market definition, market id, datetime fields
        instead of strings, and the unique _id field for MongoDB. The metadata base
        returned will be extended as the market data is processed (ie. ladder is built)

        Parameters:
        - mc (dict): The last market change update.

        Returns:
        - tuple: A tuple containing the base metadata dictionary, the market ID, and the event ID.
        """
        
        metadata = copy.deepcopy(mc)
        market_id = mc["id"]
        metadata["marketId"] = market_id
        del metadata["id"]

        # Set _id to avoid duplicates when inserting into MongoDB
        event_id = metadata["marketDefinition"]["eventId"]
        metadata["_id"] = f"metadata_{market_id}_{event_id}"
        metadata["eventId"] = event_id

        # Convert date strings to datetime objects
        metadata["marketDefinition"]["openDate"] = datetime.strptime(metadata["marketDefinition"]["openDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
        metadata["marketDefinition"]["marketTime"] = datetime.strptime(metadata["marketDefinition"]["marketTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
        metadata["marketDefinition"]["suspendTime"] = datetime.strptime(metadata["marketDefinition"]["suspendTime"], "%Y-%m-%dT%H:%M:%S.%fZ")

        return metadata, market_id, event_id
    

    def extend_pre_market(self, ts_ladder_document: dict, pt: pd.Timestamp):
        """
        Extend the metadata with the pre0ladder, pre5ladder, pre10ladder fields,
        where each field is the full ladder containing all runners at the specified
        time (ie. 10 minutes, 5 minutes and 1 second before the market goes in-play.
        A deep copy of the ladders is taken to avoid circular converion error when
        Mongo attemps to convert `ts_document["metadata"]` into BSON.

        Parameters:
        - ts_ladder_document (dict): The ladder document at the specified time.
        - pt (pd.Timestamp): The timestamp of the ladder document.
        """

        # Don't calculate pre0ladder, pre5ladder, pre10ladder if in_play_start is None
        # this handles the edge case where the market never goes in-play
        if not self.in_play_start:
            return

        milliseconds_to_in_play = (self.in_play_start - pt).total_seconds() * 1000

        if not self.pre0ladder and milliseconds_to_in_play < ONE_SECOND:
            # Technically, milliseconds_to_in_play == 0 would work because pt_date
            # will eventually be equal to the timestamp returned by get_in_play_pt()
            # but it's a bit scary to compare them in case of floating point errors
            self.pre0ladder = ts_ladder_document
            self.metadata["pre0ladder"] = copy.deepcopy(self.pre0ladder["runners"])

        if not self.pre5ladder and milliseconds_to_in_play < FIVE_MINUTES:
            self.pre5ladder = ts_ladder_document
            self.metadata["pre5ladder"] = copy.deepcopy(self.pre5ladder["runners"])

        if not self.pre10ladder and milliseconds_to_in_play < TEN_MINUTES:
            self.pre10ladder = ts_ladder_document
            self.metadata["pre10ladder"] = copy.deepcopy(self.pre10ladder["runners"])


    def extract_winner_info(self, runner_names: list[str], runner_ids: list[str], status: list[str], bsp: list[float]) -> dict:
        """
        Extracts the winner's information from the given runner details.

        Parameters:
        - runner_names (list[str]): The list of runner names.
        - runner_ids (list[str]): The list of runner IDs.
        - status (list[str]): The list of runner statuses.
        - bsp (list[float]): The list of Betfair Starting Prices (BSP) for each runner.

        Returns:
        - dict[str, Union[str, float]]: A dictionary containing the winner's name, ID, and BSP (if available). Returns None if no winner is found.

        Example:
        >>> Input: runner_names = ['Runner A', 'Runner B'], runner_ids = [123, 456], status = ['LOSER', 'WINNER'], bsp = [5.0, 3.0]
        >>> Output: {'name': 'Runner B', 'id': 456, 'bsp': 3.0}
        """

        if 'WINNER' not in status:
            return

        winner_index = status.index('WINNER')        
        winner_info = {
            'name': runner_names[winner_index],
            'id': runner_ids[winner_index],
        }

        if bsp:
            winner_info['bsp'] = bsp[winner_index]

        return winner_info
    

    def extract_favourite_info(self, runner_names: list[str], runner_ids: list[str], bsp: list[float]) -> list[dict]:
        """
        Extracts information for each runner considered a favourite based on the lowest Betfair Starting Prices (BSP).

        Parameters:
        - runner_names (list[str]): The list of runner names.
        - runner_ids (list[str]): The list of runner IDs.
        - bsp (list[float]): The list of BSPs for each runner.

        Returns:
        - list[dict[str, Union[str, float]]]: A list of dictionaries, each containing a favourite's name, ID, and BSP. Sorted by BSP in ascending order.

        Example:
        >>> Input: runner_names = ['Runner A', 'Runner B'], runner_ids = [123, 456], bsp = [5.0, 3.0]
        >>> Output: [{'name': 'Runner B', 'id': 456, 'bsp': 3.0}, {'name': 'Runner A', 'id': 123, 'bsp': 5.0}]
        """

        if not bsp:
            return

        bsp_tuples = [(index, bsp) for index, bsp in enumerate(bsp) if bsp]
        bsp_tuples.sort(key=lambda x: x[1])

        return [
            {
                'name': runner_names[index], 
                'id': runner_ids[index], 
                'bsp': bsp
            } for index, bsp in bsp_tuples
        ]
        
    
    def extend_post_market(self):
        """
        Extends the metadata with winner and favourite information based on the runners' details after the market
        has ended. This method processes the list of runners, extracting their names, IDs, statuses, and BSPs to
        determine the winner and the favourites. The winner is determined based on the status, and favourites are
        identified based on the lowest BSPs. Additionally, this method calls a subclass-specific method
        `_extend_post_market` to allow for sport-specific extensions of the metadata.
        """
        
        market_name: str = self.metadata['marketDefinition']['name']
        runners: list[dict] = self.metadata['marketDefinition']['runners']
        runner_names, runner_ids, status, bsps = [], [], [], []

        for runner in runners:
            runner_names.append(runner['name'])
            runner_ids.append(runner['id'])
            status.append(runner['status'])

            if runner.get('bsp'):
                bsps.append(runner['bsp'])

        # Only add fields to mongo document if they exist and are not None
        # When querying mongo, it deals with missing fields better than None
        winner_info: dict = self.extract_winner_info(runner_names, runner_ids, status, bsps)
        if winner_info:
            self.metadata['winnerInfo'] = winner_info

        favourite_info: dict = self.extract_favourite_info(runner_names, runner_ids, bsps)
        if favourite_info:
            self.metadata['favouriteInfo'] = favourite_info

        # Sport specific metadata extension
        self._extend_post_market(market_name, runner_names, runner_ids, status, bsps)

        
    def _extend_post_market(self, market_name: str, runner_names: list[str], runner_ids: list[str], status: list[str], bsps: list[float]):
        raise NotImplementedError
    

    def get(self):
        return self.metadata
    

class HorseRacingMetadataBuilder(MetadataBuilder):
    """
    A subclass of MetadataBuilder tailored for building metadata for horse racing markets.
    This class extends the base metadata with horse racing-specific information such as
    race type, distance, and handicap status.
    """

    def __init__(self, marketdata: list[dict], in_play_start: pd.Timestamp, in_play_end: pd.Timestamp):
        super().__init__(marketdata, in_play_start, in_play_end)


    def _extend_post_market(self, market_name: str, runner_names: list[str], runner_ids: list[str], status: list[str], bsps: list[float]):
        """
        Extends the metadata with horse racing-specific information for WIN markets. This includes
        the race type (e.g., Hurdle, Chase), distance, and whether it is a handicap race.

        Parameters:
        - market_name (str): The name of the market.
        - runner_names (list[str]): List of runner names.
        - runner_ids (list[str]): List of runner IDs.
        - status (list[str]): List of runner statuses.
        - bsps (list[float]): List of Betfair Starting Prices for the runners.
        """

        # Fields that are processed and appended to the metadata
        # below are only available for WIN markets for horse racing
        if self.metadata['marketDefinition']['marketType'] == MarketType.WIN.value:
            self.metadata['raceTypeAdjusted'] = self.extract_race_type_adjusted(market_name)
            self.metadata['distance'] = self.extract_distance(market_name)
            self.metadata['isHandicap'] = self.is_handicap(market_name)


    def extract_race_type_adjusted(self, name: str) -> str:
        """
        Determines the race type based on the market name.

        Parameters:
        - name (str): The name of the market.

        Returns:
        - str: The adjusted race type (e.g., Hurdle, Chase, NH Flat, Flat).
        """

        if 'Hrd' in name or 'Hurdle' in name:
            return 'Hurdle'
        elif 'Chs' in name or 'Chase' in name:
            return 'Chase'
        elif 'INHF' in name:  
            return 'NH Flat'
        else:  
            return 'Flat'
        

    def extract_distance(self, name: str) -> str:
        """
        Extracts the race distance from the market name using regular expressions.

        Parameters:
        - name (str): The name of the market.

        Returns:
        - str: The race distance (e.g., "2m4f") if found, otherwise an empty string.
        """

        # Regex to match the distance in metadat['marketDefinition']['name']
        match = re.search(r'(\d+m\d*f|\d+m|\d+f)', name)
        if match:
            return match.group(0)
    

    def is_handicap(self, name: str) -> bool:
        """
        Determines whether the race is a handicap race based on the market name.

        Parameters:
        - name (str): The name of the market.

        Returns:
        - bool: True if the race is a handicap race, False otherwise.
        """

        return 'Hcap' in name or 'Handicap' in name
    

class FootballMetadataBuilder(MetadataBuilder):
    def __init__(self, marketdata: list[dict], in_play_start: pd.Timestamp, in_play_end: pd.Timestamp):
        super().__init__(marketdata, in_play_start, in_play_end)

    
    def _extend_post_market(self, market_name: str, runner_names: list[str], runner_ids: list[str], status: list[str], bsps: list[float]):
        pass


class TennisMetadataBuilder(MetadataBuilder):
    def __init__(self, marketdata: list[dict], in_play_start: pd.Timestamp, in_play_end: pd.Timestamp):
        super().__init__(marketdata, in_play_start, in_play_end)

    
    def _extend_post_market(self, market_name: str, runner_names: list[str], runner_ids: list[str], status: list[str], bsps: list[float]):
        pass