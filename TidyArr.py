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


async def get_qbittorrent_data(qb_table: TinyDB.table) -> Optional[List[dict]]:
    qb_table = db_pool.table("qbittorrent")
    try:
        async with aiohttp.ClientSession() as session:
            qb_client = qbittorrentapi.Client(
                host=QB_HOSTNAME,
                port=QB_PORT,
                username=QB_USERNAME,
                password=QB_PASSWORD,
            )
            qb_client.auth_log_in()
            torrents = qb_client.torrents_info(session=session)
            qb_data = [
                {
                    "name": t["name"],
                    "status": t["state"],
                    "seeds": t["num_seeds"],
                    "peers": t["num_leechs"],
                    "percentage_completed": t["progress"],
                }
                for t in torrents
                if t["state"] not in ["stalledUP", "uploading", "completed"]
            ]
            qb_table.truncate()
            qb_table.insert_multiple(qb_data)
            return qb_data
                
    except (qbittorrentapi.LoginFailed, qbittorrentapi.APIConnectionError) as e:
        print(f"Failed to connect to qbittorrent: {e}")
        qb_table.truncate()
        return None

    except Exception as e:
        print(f"An error occurred during qbittorrent data update: {e}")
        qb_table.truncate()
        return None


async def get_endpoint_data(endpoint: str) -> Optional[Dict[str, Any]]:
    endpoint_url = ENDPOINTS[endpoint]["url"]
    params = {"apikey": ENDPOINTS[endpoint]["api_key"], "pageSize": 10000}

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=10)) as session:
        async with session.get(endpoint_url, params=params) as response:
            try:
                response.raise_for_status()
                data = await response.json()
            except (aiohttp.ClientError, ValueError) as exc:
                logging.exception("Error fetching data for %s: %s", endpoint, str(exc))
                return None

    return data


async def update_arr_endpoint_data(endpoint: str, table: TinyDB.table, db_pool: TinyDB, qb_table: TinyDB.table) -> None:
    data: Optional[Dict[str, Any]] = await get_endpoint_data(endpoint)
    qbittorrent_data = qb_table.all()

    existing_ids: Set[int] = {item["id"] for item in table}
    logging.debug("Existing items: %s", existing_ids)

    # Find the new and updated items
    new_items = []
    updated_items = []
    for item in data["records"]:
        if item["id"] not in existing_ids:
            new_items.append(item)
        else:
            updated_items.append(item)

    # Combine the new and updated items
    endpoint_items = new_items + updated_items

    # Find the IDs of the endpoint items
    endpoint_ids = [item["id"] for item in endpoint_items]
    logging.debug("Endpoint IDs: %s", endpoint_ids)

    # Find the IDs of the items that are missing from the endpoint
    missing_ids = existing_ids - set(endpoint_ids)
    logging.debug("Missing item IDs: %s", missing_ids)

    # Remove the missing items from the table
    items_to_remove = table.search(Query().id.one_of(missing_ids))
    logging.debug("Items to remove: %s", items_to_remove)
    await remove_missing_from_db(table, items_to_remove)

    # Find the items that have been inactive for longer than the threshold
    inactive_items = table.search(Query().inactiveCount >= INACTIVE_THRESHOLD)
    logging.debug("Inactive items: %s", inactive_items)

    # Remove the inactive items from the endpoint
    inactive_ids = [item["id"] for item in inactive_items]
    for item_id in inactive_ids:
        await remove_inactive_from_endpoint(table, item_id)

    # Upsert the new and updated items
    for item in endpoint_items:
        # Filter out items in a delay or completed status
        if item["status"] in {"delay", "completed"}:
            continue

        # Check if the item already exists in the database
        existing_item = table.get(Query().id == item["id"])

        # Update the item with the qbittorrent data
        matching_qb_item = await match_and_update_qbittorrent_data(item, qbittorrent_data)

        # Increment the inactiveCount
        item = await increment_inactive_count(item, existing_item)

        # Check if the item data has changed
        if existing_item != item:
            # Update the item in the database
            timestamp = await get_current_timestamp()
            item.update({"inactiveCount": 0})
            item_dict = {
                "id": item["id"],
                "title": item["title"],
                "status": item["status"],
                "sizeleft": item["sizeleft"],
                "inactiveCount": item["inactiveCount"],
                "timestamp": timestamp,
                "qbittorrent": item.get("qbittorrent"),
            }
            table.upsert(item_dict, Query().id == item["id"])
            if matching_qb_item:
                logging.debug(
                    "Successfully upserted item - Endpoint: %s - ID: %s - Title: %s",
                    endpoint,
                    item["id"],
                    item["title"],
                )
            else:
                logging.debug(
                    "Successfully upserted item - Endpoint: %s - ID: %s - Title: %s",
                    endpoint,
                    item["id"],
                    item["title"],
                )

    db_pool.update({"table": table.all()}, Query().table_name == endpoint)


async def match_and_update_qbittorrent_data(item: dict, qbittorrent_data: List[dict]) -> dict:
    # Find exact matching items in the qbittorrent data based on the name field
    matching_qb_items = [
        qb_item for qb_item in qbittorrent_data if qb_item["name"] == item["title"]
    ]

    # If there are no exact matches, try fuzzy matching
    if not matching_qb_items:
        # Find matching items in the qbittorrent data based on a partial match of the name field
        matching_qb_items = [
            qb_item
            for qb_item in qbittorrent_data
            if fuzz.partial_ratio(qb_item["name"], item["title"]) > 99
        ]

        if matching_qb_items:
            logging.debug(
                "Fuzzy matching successful - Item Title: %s - QBittorrent Title: %s",
                item["title"],
                matching_qb_items[0]["name"],
            )
        else:
            logging.error(
                "Fuzzy matching unsuccessful - Item Title: %s",
                item["title"],
            )

    # If there is a matching qbittorrent item, update the item with the qbittorrent data
    if matching_qb_items:
        qb_fields = ["name", "peers", "percentage_completed", "seeds", "status"]
        qb_dict = {k: matching_qb_items[0].get(k) for k in qb_fields}
        item["qbittorrent"] = qb_dict

    return item            


async def increment_inactive_count(item: dict, existing_item: Optional[dict]) -> dict:
    if existing_item is None:
        item["inactiveCount"] = 0
        return item

    total_inactive_count = 0
    if existing_item.get("status") == item.get("status") == "warning":
        if existing_item.get("sizeleft") != item.get("sizeleft"):
            item["inactiveCount"] = existing_item.get("inactiveCount", 0) // 2
            logging.debug("Sizeleft of existing item is different from sizeleft of new item. Inactive count halved.")
        else:
            total_inactive_count += 1
            logging.debug("Sizeleft of existing item is not different from sizeleft of new item. Inactive count increased by 1.")

    qbittorrent = item.get("qbittorrent", {})
    if qbittorrent.get("peers", 0) == 0:
        total_inactive_count += 1
        logging.debug("Matching qBittorrent item has no peers. Inactive count increased by 1.")
    if qbittorrent.get("seeds", 0) == 0:
        total_inactive_count += 2
        logging.debug("Matching qBittorrent item has no seeds. Inactive count increased by 2.")
    if qbittorrent.get("percentage_completed", 0) >= 0.95:
        total_inactive_count -= 2
        logging.debug("Matching qBittorrent item has a percentage completed of 95 or greater. Inactive count decreased by 2.")
    if qbittorrent.get("status") == "metaDL":
        total_inactive_count += 10
        logging.debug("Matching qBittorrent item has a status of metaDL. Inactive count increased by 10.")

    if existing_item.get("sizeleft") != item.get("sizeleft") and item.get("inactiveCount", 0) > 0:
        item["inactiveCount"] = existing_item.get("inactiveCount", 0) // 2
        logging.debug("Sizeleft of existing item is different from sizeleft of new item. Inactive count halved.")

    item["inactiveCount"] = existing_item.get("inactiveCount", 0) + total_inactive_count
    return item


async def remove_missing_from_db(table: TinyDB.table, ids_to_remove: List[int]) -> None:
    # Find the items that match the IDs to remove
    items = table.search(Query().id.one_of(ids_to_remove))

    # Remove the items from the table
    for item in items:
        table.remove(Query().id == item["id"])

        # Check if the item was successfully removed
        if table.contains(Query().id == item["id"]):
            logging.error(
                "Failed to remove missing item - ID: %s - Title: %s",
                item.get("id"),
                item.get("title"),
            )
        else:
            logging.info(
                "Successfully removed missing item - ID: %s - Title: %s",
                item.get("id"),
                item.get("title"),
            )


async def remove_inactive_from_endpoint(table: TinyDB.table, item_id: Optional[int]) -> None:
    TESTING = True

    if item_id is None:
        return

    if TESTING:
        logging.info("Skipping removal of inactive item - ID: %s", item_id)
        return

    url = f"{ENDPOINTS[table.name]['url']}/{item_id}"

    params = {
        "apikey": ENDPOINTS[table.name]["api_key"],
        "removeFromClient": "true",
        "blocklist": "true",
    }

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=10)
    ) as session:
        async with session.delete(url, params=params) as response:
            try:
                response.raise_for_status()
            except (aiohttp.ClientError, ValueError) as exc:
                logging.exception(
                    "Error removing item - Endpoint: %s - ID: %s : %s",
                    table.name,
                    item_id,
                    exc,
                )
            else:
                if response.status == 200:
                    table.remove(Query()[table.name].get("id") == item_id)
                    logging.info(
                        "Successfully removed stale item - Endpoint: %s - ID: %s",
                        table.name,
                        item_id,
                    )


async def get_current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def log_database_status(db_pool: TinyDB) -> None:
    for endpoint in ENDPOINTS:
        table: TinyDB.table = db_pool.table(endpoint)
        items: List[Dict[str, Any]] = table.all()
        status_counts: Dict[str, int] = {}
        warning_counts: List[Dict[str, Any]] = []
        inactive_counts: List[int] = []
        for item in items:
            status: str = item["status"]
            inactive_count: int = item["inactiveCount"]
            if status in status_counts:
                status_counts[status] += 1
            else:
                status_counts[status] = 1
            if status == "warning" and inactive_count > 0:
                warning_counts.append(item)
                inactive_counts.append(inactive_count)
        logging.info("Monitoring endpoint: %s - Items: %s", endpoint, len(items))
        status_output: str = ", ".join(
            [
                f"{status.capitalize()} items: {count}"
                for status, count in status_counts.items()
            ]
        )
        logging.info(status_output)
        if warning_counts:
            total_inactive: int = sum(inactive_counts)
            avg_inactive: float = total_inactive / len(inactive_counts)
            min_inactive: int = min(inactive_counts)
            max_inactive: int = max(inactive_counts)
            logging.info(
                "Warning count (%s): min=%s, max=%s, avg=%.1f",
                len(warning_counts),
                min_inactive,
                max_inactive,
                avg_inactive,
            )


async def main() -> None:
    


    try:
        while True:
            qb_items = await get_qbittorrent_data()
        
            if qb_items is not None:
                for endpoint in ENDPOINTS:
                    await update_arr_endpoint_data(
                        endpoint, db_pool.table(endpoint), db_pool, qb_table
                    )
                await log_database_status(db_pool)
            await asyncio.sleep(SCRIPT_INTERVAL)
    except Exception as exc:
        logging.exception("Error running API script: %s", exc)
        raise
    finally:
        db_pool.close()
        logging.info("API script finished.")


if __name__ == "__main__":
    try:
        logging.info("Starting script.")
        asyncio.run(main())
    except Exception as e:
        logging.exception("Error running API script: %s", e)
        raise


# Get endpoint_items from endpoint

# Items without qbittorrent.id should be matched



# Determine the category of each item - Existing 


# Add new_items to endpoint table 




# Calculate inactiveCount

# items with qbittorrent.id should be updated

# Update updated_items in endpoint table

# Remove items missing from endpoint_items


# retrieve_data_from_endpoint(): Retrieves data from the endpoint.
async def retrieve_data_from_endpoint(endpoint_name: str) -> List[Dict[str, str]]:
    """
    This function retrieves data from the external API endpoint.
    
    Args:
        endpoint_name (str): The name of the API endpoint.

    Returns:
        List[Dict[str, str]]: The retrieved data.
    """








    # retrieve_data_from_database(): Retrieves existing data from the database.
    # compare_data(): Compares new and existing data to determine which data needs to be updated, removed, or added.
    # upsert_new_data(): Upserts new and updated data into the database.
    # remove_missing_items(): Removes missing data from the database.
    # remove_inactive_items(): Removes inactive data from the database.
    # match_and_update_qbittorrent_data(): Matches and updates qBittorrent data with the data in the database.
    # update_database_pool(): Updates the database pool.




async def retrieve_data_from_endpoint(endpoint_name: str) -> List[Dict[str, str]]:
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
                    filtered_items = [{key: value for key, value in item.items() if key in {"id", "title", "status", "sizeleft"}} for item in items]
                    return filtered_items
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

async def get_bittorrent_data_from_qbittorrent(qb_table: TinyDB.table) -> List[Dict[str, str]]:


async def process_data(raw_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    This function processes the retrieved data to prepare it for comparison and insertion into the database.

    Args:
        raw_data (List[Dict[str, str]]): The raw data retrieved from the API endpoint.

    Returns:
        List[Dict[str, str]]: The processed data.
    """
    # Implementation code goes here


async def match_and_update_qbittorrent_data(processed_data: List[Dict[str, str]], qbittorrent_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    This function matches and updates the qBittorrent data with the new and existing data. 
    It takes in the processed data and the qBittorrent data as inputs, and returns the updated qBittorrent data.

    Args:
        processed_data (List[Dict[str, str]]): The processed data to be merged with the qBittorrent data.
        qbittorrent_data (List[Dict[str, str]]): The existing qBittorrent data to be updated.

    Returns:
        List[Dict[str, str]]: The updated qBittorrent data.
    """
    # Implementation code goes here


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