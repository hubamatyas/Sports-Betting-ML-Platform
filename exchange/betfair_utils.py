import json
import regex
from enum import Enum


def is_market_file(file_key: str) -> bool:
    """
    Checks if the specified file key corresponds to a market file based on its naming convention.
    
    Betfair's market data files are identified by filenames starting with "1.". This function 
    filters such files by analyzing the file key, typically a path, and determining if the actual 
    filename (the last part of the path) starts with "1.".
    
    Parameters:
    - file_key (str): The full key (including path) of the file to check.
    
    Returns:
    - bool: True if the file is a market file, False otherwise.
    
    Example:
    >>> is_market_file("horse-racing/2023/Jan/3/322919/1.23456.bz2")
    True
    >>> is_market_file("horse-racing/2023/Jan/3/322919/2.23456.bz2")
    False
    """

    file_name = file_key.split("/")[-1]
    return file_name.startswith("1.")


def json_load_updates(string_updates: list[str]) -> list[dict]:
    """
    Parses a list of market update strings in JSON format into a list of dictionaries.
    
    Each string in the input list represents a single market update. This function parses
    each of these strings into a Python dictionary and aggregates them into a list.
    
    Parameters:
    - string_updates (list[str]): A list of market updates in JSON string format.
    
    Returns:
    - list[dict]: A list of dictionaries, each representing a market update.
    
    Example:
    >>> json_load_updates(['{"mc": [{"id": "1.234"}]}', '{"mc": [{"id": "1.235"}]}'])
    >>> [{'mc': [{'id': '1.234'}]}, {'mc': [{'id': '1.235'}]}]
    """

    marketdata = []
    for line in string_updates:
        update = json.loads(line)
        marketdata.append(update)
    
    return marketdata


def get_market_definition(marketdata: list[dict]) -> dict:
    """
    Extracts the market definition from the first market data entry.
    
    The function assumes that the input list contains at least one market data entry, and it 
    extracts the market definition from the first entry. The market definition contains details 
    about the market such as its type, country, venue, etc.
    
    Parameters:
    - marketdata (list[dict]): A list of market data dictionaries.
    
    Returns:
    - dict: A dictionary containing the market definition.
    
    Example:
    >>> get_market_definition([{"mc": [{"marketDefinition": {"marketType": "WIN"}}]}])
    >>> {'marketType': 'WIN'}
    """

    return marketdata[0]["mc"][0]["marketDefinition"]


def is_matching_filters(market_definition: dict, market_filter: Enum, country_filter: Enum) -> bool:
    """
    Determines if a market definition matches given market and country filters.
    
    The function checks if the market type and country code within the market definition match
    the specified market and country filters. Filters are provided as Enums, with their `value`
    attribute containing the regular expression to match against.
    
    Parameters:
    - market_definition (dict): The market definition dictionary to check.
    - market_filter (Enum): The Enum whose value is a regular expression for filtering market types.
    - country_filter (Enum): The Enum whose value is a regular expression for filtering country codes.
    
    Returns:
    - bool: True if the market matches both filters, False otherwise.
    
    Example:
    >>> from enum import Enum
    >>> class MarketType(Enum):
    ...     WIN = "WIN"
    >>> class CountryCode(Enum):
    ...     UK = "GB"
    >>> is_matching_filters({"marketType": "WIN", "countryCode": "GB"}, MarketType.WIN, CountryCode.UK)
    True
    """

    # There are edge cases where, for example, country code is not present
    market_type = market_definition.get("marketType", "")
    country_code = market_definition.get("countryCode", "")
    if regex.search(market_filter.value, market_type) and regex.search(country_filter.value, country_code):
        return True
    
    return False