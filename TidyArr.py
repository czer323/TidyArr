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
        

async def merge_endpoint_with_qbittorrent_data(raw_endpoint_data: List[Dict[str, str]], qbittorrent_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
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
                    logging.debug("Matched endpoint item with qBittorrent item - Endpoint item: %s - qBittorrent item: %s", endpoint_item, qb_item)
                    break
            # If the item was not matched, log a warning
            if not endpoint_item["matched"]:
                logging.warning("Could not match endpoint item with qBittorrent item - Endpoint item: %s", endpoint_item)

        # Add a timestamp to each item in the processed data
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item in raw_endpoint_data:
            item["timestamp"] = timestamp

        # Change the return variable to merged_endpoint_data
        merged_endpoint_data = raw_endpoint_data
        return merged_endpoint_data

    # If there is an error, return an empty list
    except Exception as exc:
        logging.exception("Error matching and updating qBittorrent data: %s", exc)
        return []
    

async def compare_new_and_existing_data(merged_endpoint_data: List[Dict[str, Any]], existing_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    This function compares the new and existing data to determine which data needs to be added, updated, marked as missing, or reviewed for inactivity.
    The merged_endpoint_data has a key named "id" which is used to match items between the new and existing data.
    New_items are in the new data but not in the existing data.
    Missing_items are in the existing data but not in the new data.
    Existing_items are in both the new and existing data and have a matching "id".
    Existing_items will need to be reviewed for inactivity by running the check_for_inactivity function.
    We will then return four lists: new_items, updated_items, missing_items, and inactive_items.

    Args:
        merged_endpoint_data (List[Dict[str, Any]]): The merged data from the endpoint and qBittorrent.
        existing_data (List[Dict[str, Any]]): The existing data in the database.
        inactive_threshold (int): The threshold for marking an item as inactive.

    Returns:
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]: A tuple containing the data to be added, updated, marked as missing, or marked as inactive.
    """
    merged_endpoint_data_ids = [item["id"] for item in merged_endpoint_data]
    existing_data_ids = [item["id"] for item in existing_data]

    new_items = [item for item in merged_endpoint_data if item["id"] not in existing_data_ids]
    missing_items = [item for item in existing_data if item["id"] not in merged_endpoint_data_ids]
    existing_items = [item for item in merged_endpoint_data if item["id"] in existing_data_ids]

    await check_for_inactivity(existing_items, merged_endpoint_data)

    # Iterate over the existing items and check for inactivity
    inactive_items = []
    for item in existing_items:


    return new_items, updated_items, missing_items, inactive_items


async def check_for_inactivity(existing_items: List[Dict[str, Any]], merged_endpoint_data: List[Dict[str, Any]]):
    """
    This function checks for inactivity in the merged_endpoint_data items by comparing it with existing_items.  
    Multiple conditions can be matched to determine inactivity.
    If the item has an inactiveCount above 0, and the sizeleft has changed, half the inactiveCounter.
    If the item has a warning status and the sizeleft has changed, half the inactiveCounter.
    If the item has a warning status and the sizeleft has not changed, increment the inactiveCounter.
    If the item has a key value named qbittorrent and has peers or seeds with a value of 0, increment the inactiveCounter.
    If the qbittorrent status is "metaDL", increment the inactiveCounter.
    If the item has a warning status and the inactiveCounter is greater than or equal to the INACTIVE_THRESHOLD, add it to the inactive_items list.

    Args:
        existing_items (List[Dict[str, Any]]): The existing items to check for inactivity.
        INACTIVE_THRESHOLD (int): Global variable indicating the threshold for marking an item as inactive.

    Returns:
        inactive_items (List[Dict[str, Any]]): The items that have been marked as inactive.
        active_items (List[Dict[str, Any]]): The items that have not been marked as inactive.

    """
    inactive_items = []
    active_items = []

    # Iterate over the merged_endpoint_data and check conditions which indicate inactivity
    





        if item["status"] == "error" and item["inactiveCount"] >= INACTIVE_THRESHOLD:
            inactive_items.append(item)
        else:
            active_items.append(item)

    return inactive_items, active_items



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


async def remove_inactive_data_from_endpoint(endpoint_name: str, query_params: Dict[str, str]):
    """
    This function removes inactive data from the database.

    Args:
        endpoint_name (str): The connection string for the database.
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



async def main() -> None:
    """
    This function is the main function for the script.
    """

    qbittorrent_data = await get_data_from_qbittorrent()

    # Create a list of tasks for each endpoint
    tasks = []
    for endpoint_name in ENDPOINTS:
        # Retrieve data from the endpoint
        endpoint_url = endpoint_name["url"]
        endpoint_api_key = endpoint_name["api_key"]
        raw_endpoint_data = await get_data_from_endpoint(endpoint_url, endpoint_api_key)

        # Process the retrieved data
        merged_endpoint_data = await merge_endpoint_with_qbittorrent_data(raw_endpoint_data, qbittorrent_data)

        # Retrieve existing data from the database
        existing_data = DB_POOL.table(endpoint_name)

        # Compare the new and existing data
        new_data, update_data, missing_data, inactive_data = await compare_new_and_existing_data(merged_endpoint_data, existing_data)

        # Create tasks for each database operation
        tasks.append(upsert_new_and_updated_data_into_database(endpoint_name, update_data, new_data))
        tasks.append(remove_missing_data_from_database(endpoint_name, missing_data))
        tasks.append(remove_inactive_data_from_endpoint(endpoint_name, inactive_data))
        tasks.append(update_database_pool(endpoint_name, query_params))

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