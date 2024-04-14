from datetime import datetime
import pandas as pd
from enum import Enum

BETFAIR_TO_FOOTYSTATS_MAPPING = {
    "Tottenham Hotspur": "Tottenham",
    "Aston Villa": "Aston Villa"
}

class MarketType(Enum):
    WIN = "WIN"
    EACH_WAY = "EACH_WAY"
    MATCH_ODDS = "MATCH_ODDS"
    OVER_UNDER_25 = "OVER_UNDER_25"

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value

class FootystatsToBetfair():
    def __init__(self, home_team: str, away_team: str, timestamp: pd.Timestamp, market_type: Enum) -> None:
        self.timestamp = timestamp
        self.market_type = market_type
        betfair_home_team = BETFAIR_TO_FOOTYSTATS_MAPPING[home_team]
        betfair_away_team = BETFAIR_TO_FOOTYSTATS_MAPPING[away_team]
        self.event_name = f"{betfair_home_team} v {betfair_away_team}"


    def create_mongo_query(self) -> dict:
        return {
            "marketDefinition.eventName": self.event_name,
            "marketDefinition.marketType": self.market_type.value,
            "marketDefinition.openDate": {"$gte": self.timestamp, "$lt": self.timestamp + pd.Timedelta(days=1)}
        }


    def __repr__(self) -> str:
        return f"QP: {self.event_name}, {self.timestamp}"