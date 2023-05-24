"""
This script retrieves data from Sonarr and Radarr and merges it with data from qBittorrent.
It then updates the database accordingly.

This script is intended to be run on a schedule using a scheduler such as cron.

The following environment variables must be set:

    QB_HOSTNAME: The hostname or IP address of the qBittorrent API.
    QB_PORT: The port of the qBittorrent API.
    QB_USERNAME: The username of the qBittorrent API.
    QB_PASSWORD: The password of the qBittorrent API.

The following environment variables can optionally be set:

    SCRIPT_INTERVAL: The interval at which the script should run, in seconds. Default is 600 seconds (10 minutes).
    INACTIVE_THRESHOLD: The number of hours after which a record is considered inactive and should be removed from the database. Default is 72 hours (3 days).

Example usage:

    python TidyArr.py

"""

import os
import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
import json
import aiohttp
from tinydb import Query, TinyDB
import qbittorrentapi
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

# import .env file
load_dotenv()

# Create a dictionary to store all the endpoints
ENDPOINTS = {}

# Loop through all environment variables and add any that match the naming convention
for key, value in os.environ.items():
    if key.startswith("ENDPOINT_") and key.endswith("_URL"):
        name = key.split("_")[1]
        api_key = os.environ.get(f"ENDPOINT_{name}_API_KEY")
        ENDPOINTS[name] = {"url": value, "api_key": api_key}


# dotenv Configuration
QB_HOSTNAME: str = os.environ.get("QB_HOSTNAME")
QB_PORT: int = int(os.environ.get("QB_PORT"))
QB_USERNAME: str = os.environ.get("QB_USERNAME")
QB_PASSWORD: str = os.environ.get("QB_PASSWORD")
SCRIPT_INTERVAL: int = int(os.environ.get("SCRIPT_INTERVAL") or 600)
INACTIVE_THRESHOLD: int = int(os.environ.get("INACTIVE_THRESHOLD") or 72)
LOG_LEVEL: str = os.environ.get("LOG_LEVEL") or "INFO"


SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]
DB_NAME = f"{SCRIPT_NAME}.json"
LOG_NAME = f"{SCRIPT_NAME}.log"


DB_POOL = TinyDB(DB_NAME, sort_keys=True, indent=4, separators=(",", ": "))


# Create a logger that rotates the log file every day
logger = logging.getLogger(SCRIPT_NAME)
logger.setLevel(logging.DEBUG)
handler = logging.handlers.TimedRotatingFileHandler(LOG_NAME, when="midnight", backupCount=7)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)



async def get_data_from_endpoint(endpoint_name: str) -> List[Dict[str, str]]:
    """
    Retrieve data from an external API endpoint.
    Args:
        endpoint_name (str): The name of the API endpoint.
    Returns:
        List[Dict[str, str]]: The retrieved data.
    """
    # Retrieve the endpoint URL and API key from the ENDPOINTS dict
    url = ENDPOINTS[endpoint_name]["url"] + "/api/v3/queue"
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
                    response_data = await response.json(content_type=None)

                    # Extract the records from the response data
                    records = response_data["records"]

                    # Remove records that have a status of "delay" or "completed"
                    records = [item for item in records if item["status"] not in {"delay", "completed"}]

                    # When we get the record, we need to deduplicate items that have the same downloadID.  We should look for the id with the lowest value and keep that record.
                    # We can do this by creating a dictionary with the downloadID as the key and the record as the value.  Then we can sort the dictionary by the downloadID and keep the first item.
                    records_dict = {item["downloadId"]: item for item in records}
                    records = sorted(records_dict.values(), key=lambda item: item["downloadId"])

                    raw_endpoint_data = [{key: value for key, value in item.items() if key in {"id", "title", "status", "sizeleft", "downloadId"}} for item in records]

                    return raw_endpoint_data or []

                else:
                    logger.error(
                        "Error retrieving data from endpoint - Endpoint: %s - Status: %s",
                        endpoint_name,
                        response.status,
                    )
                    return []
        except (aiohttp.ClientError, json.JSONDecodeError) as exc:
            logger.exception("Error retrieving data from endpoint - Endpoint: %s : %s", endpoint_name, exc)
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
                    "qb_status": t["state"],
                    "seeds": t["num_seeds"],
                    "peers": t["num_leechs"],
                    "percentage_completed": t["progress"],
                }
                for t in torrents
            ]
            return filtered_torrents or []

    except (qbittorrentapi.LoginFailed, qbittorrentapi.APIConnectionError) as exc:
        logger.exception("Error logging into or connecting to qBittorrent: %s", exc)
        return []

    except Exception as exc:
        logger.exception("Error retrieving data from qBittorrent: %s", exc)
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
                    logger.debug("Matched endpoint item with qBittorrent item - Endpoint item: %s - qBittorrent item: %s", endpoint_item, qb_item)
                    break
            # If the item was not matched, log a warning
            if not endpoint_item["matched"]:
                logger.warning("Could not match endpoint item with qBittorrent item - Endpoint item: %s", endpoint_item)

        # Add a timestamp to each item in the processed data
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item in raw_endpoint_data:
            item["timestamp"] = timestamp

        # Change the return variable to merged_endpoint_data
        merged_endpoint_data = raw_endpoint_data
        return merged_endpoint_data or []

    # If there is an error, return an empty list
    except Exception as exc:
        logger.exception("Error matching and updating qBittorrent data: %s", exc)
        return []


async def compare_new_and_existing_data(merged_endpoint_data: List[Dict[str, Any]], existing_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    This function compares the new and existing data to determine which data needs to be new_items, active_items, missing_items, or inactive_items.
    The merged_endpoint_data has a key named "id" which is used to match items between the new and existing data.
    New_items are in the new data but not in the existing data.
    Missing_items are in the existing data but not in the new data.
    Existing_items are in both the new and existing data and have a matching "id".
    Existing_items will be passed to the check_for_inactivity function and will return a list of inactive_items and updated_items.
    We will then return four lists: new_items, updated_items, missing_items, and inactive_items.  If the list is empty, we will return None.

    Args:
        merged_endpoint_data (List[Dict[str, Any]]): The merged data from the endpoint and qBittorrent.
        existing_data (List[Dict[str, Any]]): The existing data in the database.

    Returns:
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]: A tuple containing the data to be added, updated, marked as missing, or marked as inactive.
    """
    merged_endpoint_data_ids = [item["id"] for item in merged_endpoint_data]
    existing_data_ids = [item["id"] for item in existing_data]

    new_items = [item for item in merged_endpoint_data if item["id"] not in existing_data_ids]
    missing_items = [item for item in existing_data if item["id"] not in merged_endpoint_data_ids]
    existing_items = [item for item in merged_endpoint_data if item["id"] in existing_data_ids]

    # Add a key named "inactiveCount" to each item in the new_items
    for new_item in new_items:
        new_item["inactiveCount"] = 0

    ## something is happening here where we aren't passing the correct data to check_for_inactivity.  We should be passing the existing_items, not the merged_endpoint_data.
    inactive_items, active_items = await check_for_inactivity(existing_items, existing_data)

    if new_items:
        logger.debug("New items: %s", new_items)
    if active_items:
        logger.debug("Active items: %s", active_items)
    if missing_items:
        logger.debug("Missing items: %s", missing_items)
    if inactive_items:
        logger.debug("Inactive items: %s", inactive_items)

    # Return the lists in a tuple, and if any list is empty - return None
    return new_items or [], active_items or [], missing_items or [], inactive_items or []


async def check_for_inactivity(merged_endpoint_data: List[Dict[str, Any]], existing_items: List[Dict[str, Any]]):
    """
    This function checks for conditions between the merged_endpoint_data items and existing_items to determine how much the inactiveCounter should be incremented.
    Multiple conditions can be matched to determine inactivity and use weighted scoring to increment the inactiveCounter.
    If the existing_item has an inactiveCount above 0, and the sizeleft has changed, half the inactiveCounter.
    If the merged_endpoint_data item has a stalledDL qb_status and the sizeleft has changed, half the inactiveCounter.
    If the merged_endpoint_data item has a stalledDL qb_status and the sizeleft has not changed, increment the inactiveCounter.
    If the merged_endpoint_data item has a key value named qbittorrent and has peers or seeds with a value of 0, increment the inactiveCounter.
    If the merged_endpoint_data qbittorrent qb_status is "metaDL", increment the inactiveCounter.
    If the merged_endpoint_data item's inactiveCounter is greater than or equal to the INACTIVE_THRESHOLD, add it to the inactive_items list.

    Args:
        existing_items (List[Dict[str, Any]]): The existing items to check for inactivity.
        INACTIVE_THRESHOLD (int): Global variable indicating the threshold for identified an item as inactive.

    Returns:
        inactive_items (List[Dict[str, Any]]): The items that have been identified as inactive.
        active_items (List[Dict[str, Any]]): The items that have not been identified as inactive.

    """

    inactive_items = []
    active_items = []

    # Copy key named "inactiveCount" from existing_items to merged_endpoint_data if it has a matching "id"
    for existing_item in existing_items:
        for merged_endpoint_item in merged_endpoint_data:
            if existing_item["id"] == merged_endpoint_item["id"]:
                if "inactiveCount" in existing_item:
                    merged_endpoint_item["inactiveCount"] = existing_item["inactiveCount"]
                else:
                    merged_endpoint_item["inactiveCount"] = 0

    # Check for conditions to determine inactivity
    for existing_item in existing_items:
        for merged_endpoint_item in merged_endpoint_data:
            if existing_item["id"] == merged_endpoint_item["id"]:
                if existing_item["inactiveCount"] > 0:
                    if existing_item["sizeleft"] != merged_endpoint_item["sizeleft"]:
                        merged_endpoint_item["inactiveCount"] /= 2
                        logger.warning("Inactive count for item %s decreased to %s due to sizeleft change", merged_endpoint_item['title'], merged_endpoint_item['inactiveCount'])
                    else:
                        merged_endpoint_item["inactiveCount"] *= 1.5
                        logger.warning("Inactive count for item %s doubled (Total: %s) due to no sizeleft change", merged_endpoint_item['title'], merged_endpoint_item['inactiveCount'])
                if merged_endpoint_item["qb_status"] == "stalledDL":
                    if existing_item["sizeleft"] != merged_endpoint_item["sizeleft"]:
                        merged_endpoint_item["inactiveCount"] /= 2
                        logger.warning("Inactive count for item %s decreased to %s due to sizeleft change", merged_endpoint_item['title'], merged_endpoint_item['inactiveCount'])
                    else:
                        merged_endpoint_item["inactiveCount"] += 1
                        logger.warning("Inactive count for item %s increased by 1 (Total: %s) due to stalledDL qb_status", merged_endpoint_item['title'], merged_endpoint_item['inactiveCount'])

                if "qbittorrent" in merged_endpoint_item:
                    if merged_endpoint_item["qbittorrent"]["peers"] == 0:
                        merged_endpoint_item["inactiveCount"] += 1
                        logger.warning("Inactive count for item %s increased by 1 (Total: %s) due to 0 peers", merged_endpoint_item['title'], merged_endpoint_item['inactiveCount'])
                    if merged_endpoint_item["qbittorrent"]["seeds"] == 0:
                        merged_endpoint_item["inactiveCount"] += 2
                        logger.warning("Inactive count for item %s increased by 2 (Total: %s) due to 0 seeds", merged_endpoint_item['title'], merged_endpoint_item['inactiveCount'])
                    if merged_endpoint_item["qbittorrent"]["qb_status"] == "metaDL":
                        merged_endpoint_item["inactiveCount"] += 10
                        logger.warning("Inactive count for item %s increased by 10 (Total: %s) due to metaDL qb_status", merged_endpoint_item['title'], merged_endpoint_item['inactiveCount'])

                # If the item's inactiveCounter is greater than or equal to the INACTIVE_THRESHOLD, add it to the inactive_items list.
                if merged_endpoint_item["inactiveCount"] >= INACTIVE_THRESHOLD:
                    inactive_items.append(merged_endpoint_item)
                    logger.warning("Item %s added to inactive_items list", merged_endpoint_item['title'])
                # If the item's inactiveCounter is less than the INACTIVE_THRESHOLD, add it to the active_items list.
                else:
                    active_items.append(merged_endpoint_item)
                    logger.debug("Item %s added to active_items list", merged_endpoint_item['title'])

    return inactive_items or [], active_items or []


async def upsert_new_and_updated_data_into_database(db_pool: TinyDB.table, endpoint_name: str, active_items: List[Dict[str, str]], new_items: List[Dict[str, str]]):
    """
    This function upserts the new and updated data into the TinyDB database table named after the endpoint.
    New items are added to the database.  Updated items are updated in the database.

    Args:
        endpoint_name (str): The connection string for the database.
        active_items (List[Dict[str, str]]): The data to be updated in the database.
        new_items (List[Dict[str, str]]): The data to be added to the database.

    Returns:
    """
    # Define a query object for searching the database
    query = Query()

    # Update active items in the database
    if active_items is not None:
        for item in active_items:
            existing_item = db_pool.table(endpoint_name).get(query.id == item['id'])
            if existing_item is not None:
                db_pool.table(endpoint_name).update(item, query.id == item['id'])
                logger.debug("Item %s updated in database", item['title'])

    # Add new items to the database
    if new_items is not None:
        for item in new_items:
            existing_item = db_pool.table(endpoint_name).get(query.id == item['id'])
            if existing_item is None:
                db_pool.table(endpoint_name).insert(item)
                logger.debug("Item %s added to database", item['title'])


async def remove_missing_data_from_database(db_pool: TinyDB.table, endpoint_name: str, missing_items: List[Dict[str, str]]):
    """
    This function removes missing data from the database.

    Args:
        database_connection (str): The connection string for the database.
        data_to_remove (List[Dict[str, str]]): The data to be removed from the database.

    Returns:

    """
    # Define a query object for searching the database
    query = Query()

    # Remove missing items from the database
    for item in missing_items:
        existing_item = db_pool.table(endpoint_name).get(query.id == item['id'])
        if existing_item is not None:
            db_pool.table(endpoint_name).remove(query.id == item['id'])
            logger.debug("Item %s removed from database", item['title'])
        else:
            logger.debug("Item %s not found in database", item['title'])


async def remove_inactive_data_from_endpoint(db_pool: TinyDB.table, endpoint_name: str, inactive_items: Dict[str, str]):
    """
    This function connects to the sonarr or radarr endpoint based on the endpoint_name performs a delete operation on the inactive items.
    We should expect a 200 response code if the delete operation was successful.
    We will need to pass some paramaters to the endpoint to connect to the correct endpoint and add the item to the blocklist.
    
    Args:
        endpoint_name (str): The connection string for the database.
        inactive_items (Dict[str, str]): List of items that have been identified as inactive.

    Returns:
    
    """
    # inactive_items_removed = []
    # get the id of the item

    # Define a query object for searching the database
    query = Query()

    for item in inactive_items:
        item_id = item['id']

        url = f"{ENDPOINTS[endpoint_name]['url']}/{item_id}"

        params = {
            "apikey": ENDPOINTS[endpoint_name]['api_key'],
            "removeFromClient": "true",
            "blocklist": "true",
        }

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, params=params) as response:
                if response.status == 200:
                    logger.warning("Item %s removed from %s", item['title'], endpoint_name)
                    db_pool.table(endpoint_name).remove(query.id == item['id'])
                    logger.debug("Item %s removed from database", item['title'])

                else:
                    logger.error("Item %s not removed from %s", item['title'], endpoint_name)


async def main() -> None:
    """
    This function is the main function for the script.
    """
    while True:
        logger.info("Starting script")
        qbittorrent_data = await get_data_from_qbittorrent()

        # Create a list of tasks for each endpoint
        tasks = []
        for endpoint_name in ENDPOINTS:
            # Retrieve data from the endpoint
            raw_endpoint_data = await get_data_from_endpoint(endpoint_name)

            # Process the retrieved data
            merged_endpoint_data = await merge_endpoint_with_qbittorrent_data(raw_endpoint_data, qbittorrent_data)

            # Retrieve existing data from the database
            existing_data = DB_POOL.table(endpoint_name).all()
            

            # Compare the new and existing data
            new_items, active_items, missing_items, inactive_items = await compare_new_and_existing_data(merged_endpoint_data, existing_data)

            # Create tasks for each database operation
            tasks.append(upsert_new_and_updated_data_into_database(DB_POOL, endpoint_name, active_items, new_items))
            tasks.append(remove_missing_data_from_database(DB_POOL, endpoint_name, missing_items))
            tasks.append(remove_inactive_data_from_endpoint(DB_POOL, endpoint_name, inactive_items))

        # Run all tasks concurrently
        await asyncio.gather(*tasks)

        # Close the database connection

        logger.info("Script complete")
        await asyncio.sleep(SCRIPT_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
        DB_POOL.close()

    except Exception as e:
        logger.exception("Error running API script: %s", e)
        raise
