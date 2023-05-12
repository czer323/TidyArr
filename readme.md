

```mermaid
sequenceDiagram
    participant External API
    participant Database
    participant qBittorrent
    participant retrieve_data_from_endpoint() function
    participant retrieve_data_from_database() function
    participant compare_data() function
    participant upsert_new_data() function
    participant remove_missing_items() function
    participant remove_inactive_items() function
    participant match_and_update_qbittorrent_data() function

    External API->>retrieve_data_from_endpoint(): Retrieve data from endpoint
    retrieve_data_from_endpoint()-->>compare_data(): New data
    Database->>retrieve_data_from_database(): Retrieve existing data from database
    retrieve_data_from_database()-->>compare_data(): Existing data
    compare_data()-->>upsert_new_data(): New and updated data
    upsert_new_data()-->>Database: Upserted data
    compare_data()-->>remove_missing_items(): Missing data
    remove_missing_items()-->>Database: Removed data
    compare_data()-->>remove_inactive_items(): Inactive data
    remove_inactive_items()-->>Database: Removed data
    compare_data()-->>match_and_update_qbittorrent_data(): qBittorrent data
    match_and_update_qbittorrent_data()-->>Database: Updated data
```