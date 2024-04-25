import pandas as pd
import json
from enums import MetaBuilder
from metadata import MetadataBuilder


class LadderBuilder:
    def __init__(self, marketdata: list[dict], market_definition: dict, meta_builder: MetaBuilder):
        """
        Initializes the LadderBuilder with market data, market definition, and a reference to a metadata builder.
        Attributes are initialized for storing ladders, default price values, runner IDs, the current ladder structure,
        in-play start and end times, metadata builder instance, market ID, and event ID.
        
        Parameters:
        - marketdata (list[dict]): The list of market updates.
        - market_definition (dict): A dictionary containing the market's definition details.
        - meta_builder (MetaBuilder): An instance of MetaBuilder for building metadata for the time series data.
        """

        self.marketdata = marketdata
        self.market_definition = market_definition
        
        self.ladders = []
        self.default_best_back_price = 0
        self.default_best_lay_price = float("inf")
        self.runner_ids: list[str] = self.get_runner_ids()
        self.current_ladder: dict = self.create_ladder_ds()

        self.in_play_start: pd.Timestamp = self.get_in_play_start_pt()
        self.in_play_end: pd.Timestamp = self.get_in_play_end_pt()

        # Initialize the sport specific metadata builder that was defined in main.py
        self.metadata_builder = meta_builder.value(self.marketdata, self.in_play_start, self.in_play_end)
        self.market_id = self.metadata_builder.market_id
        self.event_id = self.metadata_builder.event_id


    def list_ladder_to_dict(self, ladder: list[list]) -> dict:
        """
        Converts the list ladder Betfair provides to a dictionary to match
        the structure of self.current_ladder and to enable the use of
        ladder1.update(ladder2) to efficiently merge the dictionaries.

        Example:
        >>> Input ladder: [[2.55, 200], [2.5, 120], [2.48, 160]]
        >>> Output ladder: {2.55: 200, 2.5: 120, 2.48: 160}

        Parameters:
        - ladder (list[list]): The ladder to convert, where each sublist contains [price, amount].
        
        Returns:
        - dict: A dictionary with prices as keys and amounts as values.
        """

        return {item[0]: item[1] for item in ladder}
    

    def update_runner_ladder(self, runner_ladder: dict, runner_change: dict, price_key: str) -> dict:
        """
        Merge the current ladder and the ladder update from the Betfair runner change
        based on the price_key (atb, atl, trd). Sort the back ladder in descending order
        and the lay ladder in ascending order and only keep the top 10 prices. This is
        where the ladder update actually happens.

        Parameters:
        - runner_ladder (dict): The current ladder state for a runner.
        - runner_change (dict): The market update information for the runner.
        - price_key (str): The key to identify which part of the ladder to update ('atb', 'atl', or 'trd').
        
        Returns:
        - dict: The updated ladder for the runner.
        """

        ladder: dict = runner_ladder[price_key]
        update_list = runner_change.get(price_key, [])
        update_dict = self.list_ladder_to_dict(update_list)

        ladder.update(update_dict)
        ladder = {key: value for key, value in ladder.items() if value > 0}

        # only keep the top 10 prices
        # sorted is an expensive operation, but we are only sorting 10 items
        if price_key == "atb":
            # sort back prices in descending order
            return dict(sorted(ladder.items(), reverse=True)[:10])
        elif price_key == "atl":
            # sort lay prices in ascending order
            return dict(sorted(ladder.items())[:10])
        elif price_key == "trd":
            return dict(sorted(ladder.items()))
        

    def create_ladder_ds(self) -> dict:
        """
        Create the dictionary of dictionary structure to maintain the current ladder.
        Uses self.runner_ids to create the sub-dictionaries for each runner.
        
        Returns:
        - dict: A dictionary with runner IDs as keys, each containing a nested dictionary for ladder information.
        """

        runners = {}
        for runner_id in self.runner_ids:
            runners[runner_id] = {
                "atb": {},
                "atl": {},
                "trd": {},
                "trades": [],
                "ltp": None,
                "tv": None,
                # total traded volume on runner, currently done iteratively. This is more efficient than
                # summing the trd values in each update with: ttrdv = sum(runner_ladder["trd"].values())
                "ttrdv": 0,
                "bbp": self.default_best_back_price,
                "blp": self.default_best_lay_price,
            }
        return runners
    

    def check_arbitrage(self, runner_ladder: dict, runner_id: str, pt: pd.Timestamp) -> bool:
        """
        Checks for arbitrage opportunities for a given runner. Also sets the best back `bbp` and
        best lay prices `blp` for the runner. This is necessary, firstly, to check for arbitrage
        opportunities based on the most recent state of the runner ladder. Secondly,
        `update_runner_trades` uses the `bbp` and `blp` to determine which side of the ladder
        was hit.
        
        Parameters:
        - runner_ladder (dict): The ladder data for the runner.
        - runner_id (str): The ID of the runner.
        - pt (pd.Timestamp): The point in time the check is performed.
        
        Returns:
        - bool: True if an arbitrage opportunity is detected, False otherwise.
        """
        
        # atb and atl are already sorted
        best_back_price = runner_ladder["atb"][0][0] if runner_ladder.get("atb") else self.default_best_back_price
        best_lay_price = runner_ladder["atl"][0][0] if runner_ladder.get("atl") else self.default_best_lay_price
        self.current_ladder[runner_id]["bbp"] = best_back_price
        self.current_ladder[runner_id]["blp"] = best_lay_price

        if best_back_price > best_lay_price:
            # print("Arbitrage opportunity detected at timestamp: ", pt.timestamp())
            # print(best_back_price, best_lay_price, runner_id)
            # print(json.dumps(runner_ladder, indent=4))
            return True
        
        return False
    

    def raise_if_arbitrage(self, ladder: dict, pt: str) -> bool:
        """
        Raises an alert if arbitrage is detected in any runner's ladder.
        
        Parameters:
        - ladder (dict): The current state of the ladder for all runners.
        - pt (pd.Timestamp): The point in time the check is performed.
        """

        runner_ladders = ladder["runners"]
        for runner_id in runner_ladders.keys():
            if self.check_arbitrage(runner_ladders[runner_id], runner_id, pt):
                # raise ValueError("Arbitrage opportunity detected. Investiage further.")
                pass

    
    def update_runner_value(self, runner_ladder: dict, runner_change: dict, key: str) -> dict:
        """
        Update the single value fields in the runner ladder, such as `ltp` and `tv` based on
        the runer change.

        Parameters:
        - runner_ladder (dict): The ladder data for the runner.
        - runner_change (dict): The market update information for the runner.
        - key (str): The key of the value to update.
        """

        value = runner_change.get(key)
        return value if value else runner_ladder[key]
    

    def get_update_by_runner_id(self, runner_id: str, rc: dict, price_key: str) -> dict:
        """
        Retrieves updates for a specific runner based on runner ID and update type.
        
        Parameters:
        - runner_id (str): The ID of the runner.
        - rc (dict): The runner change data.
        - price_key (str): The key indicating the type of ladder update.
        
        Returns:
        - dict: The updates for the specified runner.
        """

        for runner_change in rc:
            if str(runner_change["id"]) == runner_id and runner_change.get(price_key):
                return self.list_ladder_to_dict(runner_change[price_key])
            
        return {}
    

    def update_runner_trades(self, runner_id: str, runner_ladder: dict, runner_change: dict) -> dict:
        """
        Update the trades dictionary in the runner ladder. Trades are stored in a dictionary
        with the key being the price and the value being the volume traded at that price.

        Parameters:
        - runner_id (str): The ID of the runner.
        - runner_ladder (dict): The ladder data for the runner.
        - runner_change (dict): The market update information for the runner.
        """

        trades = []

        current_trd: dict = runner_ladder["trd"]
        for trd_update in runner_change.get("trd", []):
            traded_price = trd_update[0]
            total_volume = trd_update[1]

            traded_volume = total_volume - current_trd.get(traded_price, 0)
            traded_volume = round(traded_volume, 2)

            # Disregard trades with a volume less than 0.0 as they are associated with
            # changes in the underlying foreign currency exchange rates. TODO This is an arbitrary
            # threshold and should be reviewed. Using 0.1 might be more appropriate but would result
            # in many small trades being recorded which might not be relevant anyway.
            if traded_volume <= 0.0:
                continue

            runner_ladder["ttrdv"] += traded_volume
            if traded_price <= runner_ladder["bbp"]:
                trades.append([traded_price, traded_volume, "b"])
            elif traded_price >= runner_ladder["blp"]:
                trades.append([traded_price, traded_volume, "l"])
            else:
                trades.append([traded_price, traded_volume, "nan"])
                
        return trades


    def update_ladder(self, market_change: dict) -> dict:
        """
        Iterate over the runner changes and update the ladder prices and volumes.
        Changes are made directly to the self.current_ladder data structure, so
        expect the it to be modified after calling this function.

        Parameters:
        - market_change (dict): The market change information.
        """

        rc = market_change["rc"]
        for runner_change in rc:
            runner_id = str(runner_change["id"])
            runner_ladder = self.current_ladder[runner_id]

            # Must update trades before trd as we need the previous trd to calculate the
            # size of trades.
            runner_ladder["trades"] = self.update_runner_trades(runner_id, runner_ladder, runner_change)
            runner_ladder["atb"] = self.update_runner_ladder(runner_ladder, runner_change, "atb")
            runner_ladder["atl"] = self.update_runner_ladder(runner_ladder, runner_change, "atl")
            runner_ladder["trd"] = self.update_runner_ladder(runner_ladder, runner_change, "trd")
            runner_ladder["ltp"] = self.update_runner_value(runner_ladder, runner_change, "ltp")
            runner_ladder["tv"] = self.update_runner_value(runner_ladder, runner_change, "tv")
            
            self.current_ladder[runner_id] = runner_ladder
    

    def update_market_definition(self, market_change: dict) -> dict:
        """
        Update the market definition if one exists inside the market change.
        
        Parameters:
        - market_change (dict): The market change information containing potential market definition updates.
        """

        if not market_change.get("marketDefinition"):
            return None

        metadata, market_id, event_id = MetadataBuilder.format_metadata(market_change)
        return metadata["marketDefinition"]


    def format_ladder(self):
        """
        Converts the atb and atl dictionaries to lists of lists for easier querying in MongoDB.
        For example, look up best price with "runners.12345.atb.0.0". Keeps trd as a dictionary
        as it is not used in the same way as atb and atl. Dicts are more efficient for lookups,
        for example, "runners.12345.trd.2.55" would return the trd volume at 2.55. 2.55 could
        be replaced for example with the best back price to get the trd volume at the best back
        price.

        Returns:
        - dict: A dictionary representing the formatted ladder, where each key is a runner ID and each
                value is a dictionary containing the formatted 'atb', 'atl', 'trd', 'ltp', 'tv', 'ttrdv',
                and 'trades' data for the runner.
        """

        output_dict = {}
        for runner, ladder in self.current_ladder.items():
            output_dict[runner] = {}

            # Only add fields to mongo document if they exist and are not None
            # When querying mongo, it deals with missing fields better than None
            if ladder["atb"]:
                output_dict[runner]["atb"] = [[k, v] for k, v in ladder["atb"].items()]

            if ladder["atl"]:
                output_dict[runner]["atl"] = [[k, v] for k, v in ladder["atl"].items()]

            if ladder["trd"]:
                # Keys in MongoDB must be strings
                output_dict[runner]["trd"] = {str(k): v for k, v in ladder["trd"].items()}

            if ladder["ltp"]:
                output_dict[runner]["ltp"] = ladder["ltp"]

            if ladder["tv"]:
                output_dict[runner]["tv"] = ladder["tv"]

            if ladder["ttrdv"]:
                output_dict[runner]["ttrdv"] = round(ladder["ttrdv"], 2)

            if ladder["trades"]:
                output_dict[runner]["trades"] = ladder["trades"]
                # ladder["trades"] = [] # Reset trades to avoid appending old trades to new ladder

            # Could add best back and lay price to mongo here, if needed. Adding them would make
            # querying the best back and lay price for each runner slightly simpler. Other than that,
            # there's no need to store them.

        return output_dict
    

    def create_ts_document(self, ladder: dict, market_definition: dict, pt: pd.Timestamp) -> dict:
        """
        Create a time series document from the self.current_ladder and market definition.
        
        Parameters:
        - ladder (dict): The formatted ladder data.
        - market_definition (dict): The market definition data.
        - pt (pd.Timestamp): The timestamp of the document.

        Returns:
        - dict: A time series document ready for MongoDB insertion, containing 'pt', 'metadata',
                'runners', and optionally 'marketDefinition'.
        """
        
        ts_document = {
                "pt": pt,
                # Note that because Python passes dictionaries by reference, the metadata will be the
                # same for all ladders once the run() has terminated. Even though self.metadata_builder.extend_post_market()
                # is only called at the very end of run(), the metadata in each ts document (created here)
                # will contain the fields appended in extend_post_market(). This is exactly the behaviour we want
                # because the metadata inside a MongoDB TimeSeries collection should never change.
                # "metadata": self.metadata_builder.get(),
                "metadata": self.market_id,
                "runners": ladder,
            }
        
        if market_definition:
            ts_document["marketDefinition"] = market_definition

        return ts_document


    def get_in_play_start_pt(self) -> pd.Timestamp:
        """
        Determines the in-play start timestamp from the market data by looping through the
        raw Betfair updates until the first inPlay: true field is found.

        Returns:
        - pd.Timestamp: The timestamp when the market goes in-play, if available.
        """

        for packet in self.marketdata:
            pt, market_change = self.process_packet(packet)
            market_definition = market_change.get("marketDefinition")
            
            if market_definition and market_definition["inPlay"]:
                return pt
            

    def get_in_play_end_pt(self) -> pd.Timestamp:
        """
        Determines the end of in-play period timestamp from the last packet of market data.

        Returns:
        - pd.Timestamp: The timestamp corresponding to the last market data packet.
        """

        pt_date, market_change = self.process_packet(self.marketdata[-1])
        return pt_date


    def get_runner_ids(self) -> list[str]:
        """
        Extracts runner IDs from the market definition.

        Returns:
        - list[str]: A list of runner IDs as strings.
        """

        runner_objects = self.market_definition["runners"]
        return [str(runner["id"]) for runner in runner_objects]
    

    def process_packet(self, packet: dict):
        """
        Processes a single packet (or update) of market data, extracting the timestamp and market change.

        Parameters:
        - packet (dict): A single packet of market data.

        Returns:
        - tuple[pd.Timestamp, dict]: A tuple containing the processed timestamp and the market change dictionary.
        """

        pt = packet["pt"]
        mc = packet["mc"]
        market_change: dict = mc[0]
        pt_date: pd.Timestamp = pd.to_datetime(pt, unit="ms")
    
        # if len(mc) != 1 then the passed in data is not a single market file
        # handling multiple markets in a single file is not in the scope of this function
        assert len(mc) == 1

        return pt_date, market_change
    

    def set_raw_marketdata(self, packet: dict, pt: pd.Timestamp) -> dict:
        """
        Updates a market data packet with a specific timestamp and market ID, preparing it for
        inseration into a Time Series MongoDB collection.

        Parameters:
        - packet (dict): The original market data packet.
        - pt (pd.Timestamp): The timestamp to set for the packet.

        Returns:
        - dict: The updated market data packet with 'pt' and 'metadata' fields set.
        """

        packet["pt"] = pt
        packet["metadata"] = self.market_id

        del packet["clk"]
        del packet["op"]
        return packet


    def reset_runner_trades(self) -> dict:
        """
        Reset the trades dictionary in the runner ladder. Trades are stored in a list
        eg. `[2.2, 34.11, "b"]` If not reset, old trades would be appended to every ladder
        update. Could reset trades inside format_ladder by setting `ladder["trades"] = []`
        but for clarity it is done here.
        """

        for runner_id, ladder in self.current_ladder.items():
            ladder["trades"] = []


    def run(self):
        """
        Executes the main logic of `LadderBuilder`. Processes market data packets to update the ladders,
        format them, create time series documents, and construct metadata document.

        Returns:
        - tuple[dict, list, list]: A tuple containing the final metadata, a list of time series documents, 
                                and the processed market data packets.
        """

        for i, packet in enumerate(self.marketdata):
            pt, market_change = self.process_packet(packet)
            self.marketdata[i] = self.set_raw_marketdata(packet, pt)

            self.update_ladder(market_change)
            market_definition = self.update_market_definition(market_change)
            formatted_ladder = self.format_ladder()
            ts_ladder_document = self.create_ts_document(formatted_ladder, market_definition, pt)

            self.raise_if_arbitrage(ts_ladder_document, pt)
            self.metadata_builder.extend_pre_market(ts_ladder_document, pt)
            self.ladders.append(ts_ladder_document)
            self.reset_runner_trades()
        
        self.metadata_builder.extend_post_market()
        return self.metadata_builder.get(), self.ladders, self.marketdata