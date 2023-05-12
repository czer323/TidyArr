import os
import asyncio
import logging
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, Dict, List, Any, Set
import json
import aiohttp
from tinydb import Query, TinyDB
import qbittorrentapi
from fuzzywuzzy import fuzz


ENDPOINTS = {
    "radarr": {
        "url": "***REMOVED***",
        "api_key": "***REMOVED***",
    },
    "sonarr": {
        "url": "***REMOVED***",
        "api_key": "***REMOVED***",
    },
}


# Configuration
QB_HOSTNAME: str = "***REMOVED***"  # Replace with your qBittorrent API hostname
QB_PORT: int = ***REMOVED***  # Replace with your qBittorrent API port
QB_USERNAME: str = "czer323"  # Replace with your qBittorrent username
QB_PASSWORD: str = "***REMOVED***"  # Replace with your qBittorrent password


SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]
DB_NAME = f"{SCRIPT_NAME}.json"
LOG_NAME = f"{SCRIPT_NAME}.log"
SCRIPT_INTERVAL = 600
INACTIVE_THRESHOLD = 72


DB_POOL = TinyDB(DB_NAME, sort_keys=True, indent=4, separators=(",", ": "))



logger = logging.getLogger(__name__)
handler = TimedRotatingFileHandler(LOG_NAME, when="midnight", backupCount=7)
logger.addHandler(handler)

logging.basicConfig(
    filename=LOG_NAME,
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)


# retrieve_data_from_qbittorrent(): Retrieves data from qBittorrent.
# retrieve_data_from_endpoint(): Retrieves data from the endpoint.
# retrieve_data_from_database(): Retrieves existing data from the database.
# compare_data(): Compares new and existing data to determine which data needs to be updated, removed, or added.
# upsert_new_data(): Upserts new and updated data into the database.
# remove_missing_items(): Removes missing data from the database.
# remove_inactive_items(): Removes inactive data from the database.
# match_and_update_qbittorrent_data(): Matches and updates qBittorrent data with the data in the database.
# update_database_pool(): Updates the database pool.




async def get_data_from_endpoint(endpoint_name: str) -> List[Dict[str, str]]:
    """
    Retrieve data from an external API endpoint.
    Args:
        endpoint_name (str): The name of the API endpoint.
    Returns:
        List[Dict[str, str]]: The retrieved data.
    """
    # Retrieve the endpoint URL and API key from the ENDPOINTS dict
    url = ENDPOINTS[endpoint_name]["url"]
    api_key = ENDPOINTS[endpoint_name]["api_key"]
    params = {"apikey": api_key, "pageSize": 10000}

    # Create a new HTTP session
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=10)) as session:
        try:
            # Send a GET request to the endpoint
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                if response.status == 200:
                    # Extract only the keys we need from each item in the JSON response
                    items = await response.json()
                    raw_endpoint_data = [{key: value for key, value in item.items() if key in {"id", "title", "status", "sizeleft"}} for item in items]
                    return raw_endpoint_data
                else:
                    logging.error(
                        "Error retrieving data from endpoint - Endpoint: %s - Status: %s",
                        endpoint_name,
                        response.status,
                    )
                    return []
        except (aiohttp.ClientError, json.JSONDecodeError) as exc:
            logging.exception("Error retrieving data from endpoint - Endpoint: %s : %s", endpoint_name, exc)
            return []



async def get_data_from_qbittorrent() -> List[Dict[str, str]]:
    """
    This function retrieves data from qBittorrent and returns it as a list of dicts containing only the keys we need.
    The keys we need are: name, state, num_seeds, num_leechs, progress.
    
    Returns:
        List[Dict[str, str]]: The retrieved data.
    """
    try:
        # Create a new qBittorrent client using asyncio
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=10)) as session:
            qb_client = qbittorrentapi.Client(
                host=QB_HOSTNAME,
                port=QB_PORT,
                username=QB_USERNAME,
                password=QB_PASSWORD
            )
            qb_client.auth_log_in(session=session)
            torrents = qb_client.torrents_info()
            filtered_torrents = [
                {
                    "name": t["name"],
                    "status": t["state"],
                    "seeds": t["num_seeds"],
                    "peers": t["num_leechs"],
                    "percentage_completed": t["progress"],
                }
                for t in torrents
            ]       
            return filtered_torrents
        
    except (qbittorrentapi.LoginFailed, qbittorrentapi.APIConnectionError) as exc:
        logging.exception("Error logging into or connecting to qBittorrent: %s", exc)
        return []
    
    except Exception as exc:
        logging.exception("Error retrieving data from qBittorrent: %s", exc)
        return []
        


async def process_data(raw_endpoint_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    This function processes the retrieved data to prepare it for comparison and insertion into the database.

    Args:
        raw_endpoint_data (List[Dict[str, str]]): The raw data retrieved from the API endpoint.

    Returns:
        List[Dict[str, str]]: The processed data.
    """
    try:
        qbittorrent_data = get_data_from_qbittorrent()
        processed_data = await match_and_update_qbittorrent_data(raw_endpoint_data, qbittorrent_data)

        # Add a timestamp to each item in the processed data
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item in processed_data:
            item["timestamp"] = timestamp
        return processed_data
    except Exception as exc:
        logging.exception("Error processing data: %s", exc)
        return []
    


async def match_and_update_qbittorrent_data(raw_endpoint_data: List[Dict[str, str]], qbittorrent_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    This function iterates over the raw_endpoint_data and attempts to match each item with an item in the qbittorrent_data.
    It then updates the endpoint data with the matched qbittorrent data and returns the updated endpoint data.
    
    Args:
        endpoint_data (List[Dict[str, str]]): The processed data to be merged with the qBittorrent data.
        qbittorrent_data (List[Dict[str, str]]): The existing qBittorrent data to be updated.

    Returns:
        List[Dict[str, str]]: The updated qBittorrent data.
    """
    try:
        # Iterate over the endpoint data and attempt to match each item using fuzzywuzzy
        for endpoint_item in raw_endpoint_data:
            endpoint_item["matched"] = False
            for qb_item in qbittorrent_data:
                # If the items match, update the endpoint item with the qbittorrent data as a key-value pair
                if fuzz.partial_ratio(endpoint_item["title"], qb_item["name"]) >= 99:
                    endpoint_item.update(qb_item)
                    endpoint_item["matched"] = True
                    break    
    
        return raw_endpoint_data
    # If there is an error, return an empty list



    except Exception as exc:
        logging.exception("Error matching and updating qBittorrent data: %s", exc)
        return []
    


async def retrieve_existing_data_from_database(database_connection: str, query_params: Dict[str, str]) -> List[Dict[str, str]]:
    """
    This function retrieves existing data from the database.

    Args:
        database_connection (str): The connection string for the database.
        query_params (Dict[str, str]): Any necessary query parameters for the database query.

    Returns:
        List[Dict[str, str]]: The retrieved data.
    """
    # Implementation code goes here


async def compare_new_and_existing_data(processed_data: List[Dict[str, str]], existing_data: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    """
    This function compares the new and existing data to determine which data needs to be updated, removed, or added.

    Args:
        processed_data (List[Dict[str, str]]): The processed data to be compared to the existing data.
        existing_data (List[Dict[str, str]]): The existing data in the database.

    Returns:
        Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]: A tuple containing the data to be updated, removed, and added.
    """
    # Implementation code goes here


async def upsert_new_and_updated_data_into_database(database_connection: str, data_to_update: List[Dict[str, str]], data_to_add: List[Dict[str, str]]):
    """
    This function upserts the new and updated data into the database.

    Args:
        database_connection (str): The connection string for the database.
        data_to_update (List[Dict[str, str]]): The data to be updated in the database.
        data_to_add (List[Dict[str, str]]): The data to be added to the database.

    Returns:

    """
    # Implementation code goes here


async def remove_missing_data_from_database(database_connection: str, data_to_remove: List[Dict[str, str]]):
    """
    This function removes missing data from the database.

    Args:
        database_connection (str): The connection string for the database.
        data_to_remove (List[Dict[str, str]]): The data to be removed from the database.

    Returns:

    """
    # Implementation code goes here


async def remove_inactive_data_from_database(database_connection: str, query_params: Dict[str, str]):
    """
    This function removes inactive data from the database.

    Args:
        database_connection (str): The connection string for the database.
        query_params (Dict[str, str]): Any necessary query parameters for the database query.

    Returns:
    
    """
    # Implementation code goes here


async def update_database_pool(database_connection: str, query_params: Dict[str, str]):
    """
    This function updates the database pool.

    Args:
        database_connection (str): The connection string for the database.
        query_params (Dict[str, str]): Any necessary query parameters for the database query.

    Returns:

    """



async def main():
    # Define the authentication credentials for the API endpoints
    auth_credentials = {"username": "user", "password": "pass"}

    # Create a list of tasks for each endpoint
    tasks = []
    for endpoint_name, endpoint_data in ENDPOINTS.items():
        # Retrieve data from the endpoint
        endpoint_url = endpoint_data["url"]
        endpoint_api_key = endpoint_data["api_key"]
        raw_data = await retrieve_data_from_endpoint(endpoint_url, endpoint_api_key)

        # Process the retrieved data
        processed_data = await process_data(raw_data)

        # Retrieve existing data from the database
        database_file = f"{endpoint_name}.db"
        query_params = {"status": "active"}
        existing_data = await retrieve_existing_data_from_database(database_file, query_params)

        # Compare the new and existing data
        data_to_update, data_to_remove, data_to_add = await compare_new_and_existing_data(processed_data, existing_data)

        # Create tasks for each database operation
        tasks.append(upsert_new_and_updated_data_into_database(database_file, data_to_update, data_to_add))
        tasks.append(remove_missing_data_from_database(database_file, data_to_remove))
        tasks.append(remove_inactive_data_from_database(database_file, query_params))
        tasks.append(update_database_pool(database_file, query_params))

    # Run all tasks concurrently
    await asyncio.gather(*tasks)


async def main_loop():
    while True:
        await main()
        await asyncio.sleep(SCRIPT_INTERVAL)

# Run the main loop in the background
loop = asyncio.get_event_loop()
task = loop.create_task(main_loop())

# Wait for user input
input("Press enter to exit...")

# Cancel the main loop task
task.cancel()

# Wait for the main loop task to finish
try:
    await task
except asyncio.CancelledError:
    pass