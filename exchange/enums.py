from enum import Enum
from metadata import HorseRacingMetadataBuilder, FootballMetadataBuilder, TennisMetadataBuilder

class MarketFilters(Enum):
    # ^ - start of the string, $ - end of the string
    FootballMarketRegex = r"(^MATCH_ODDS$)|(OVER)|(UNDER)|(_OU_)"
    TennisMarketRegex = r"(^MATCH_ODDS$)"
    HorseRacingMarketRegex = r"(^WIN$)|(^EACH_WAY$)"

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value
    

class MetaBuilder(Enum):
    Football = FootballMetadataBuilder
    Tennis = TennisMetadataBuilder
    HorseRacing = HorseRacingMetadataBuilder

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value
    

class Sport(Enum):
    Football = "football"
    Tennis = "tennis"
    HorseRacing = "horseracing"

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value
    

class CountryFilters(Enum):
    FootballCountryRegex = r".*"
    HorseRacingCountryRegex = r"(GB)|(IE)"
    TennisCountryRegex = r".*"

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value
    

class Collections(Enum):
    Metadata = "metadata"
    Ladders = "ladders"
    Marketdata = "marketdata"

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value
    

class Databases(Enum):
    Football = "football_betfair"
    Tennis = "tennis_betfair"
    HorseRacing = "horseracing_betfair"
    Betfair = "betfair_exchange"

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value


class MongoURIs(Enum):
    Test = "your_test_uri"
    Serverless = "your_serverless_uri"

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value


class AWS(Enum):
    Key = 'your_key'
    Secret = 'your_secret'
    Bucket = 'your_bucket'

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value
    