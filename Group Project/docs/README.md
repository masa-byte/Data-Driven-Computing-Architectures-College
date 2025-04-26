# Items in the docs directory
- *contracts* subdirectory
- *metadata* subdirectory
- *medallion* subdirectory

## contracts subdirectory
The contracts directory contains data contracts for the data we used in the project.
It is generated through the [Data Contracts](../code/04%20-%20Data%20Contracts.ipynb) notebook located in the code directory.
The contracts are saved in the JSON format and there is a contract for each data file used for bronze, silver and gold layers.

## metadata subdirectory
The metadata directory contains metadata for the data we used in the project.
It is generated through the [Metadata Tracking & Data Lineage](../code/03%20-%20Metadata%20Tracking%20%26%20Data%20Lineage.ipynb) notebook located in the code directory.

### golden_metadata
The golden_metadata directory contains metadata for the gold layer of the medallion architecture in the form of delta table.

### metadata_table
The metadata_table directory contains track of the data lineage from bronze to silver to gold layer and saves it as a delta table.

## medallion subdirectory
The medallion directory contains the delta tables for the bronze, silver and gold layers of the medallion architecture.
The delta tables are generated through the [Medallion](../code/01%20-%20Medallion.ipynb) notebook located in the code directory.

### bronze
The bronze directory contains the delta table for the bronze layer of the medallion architecture.
Delta tables exist for car_sales_data, companies_data, and customers_data

### silver
The silver directory contains the delta table for the silver layer of the medallion architecture.
Delta tables exist for car_sales_data, companies_data, and customers_data

### gold
The gold directory contains the delta table for the gold layer of the medallion architecture.
Delta table exists for the final joined data, partitioned by the dealer_region field.

### statistics.json
The statistics.json file contains the statistics for the ingestion process of medallion architecture. Every layer collects statistics about the data, such as the number of rows, ingestion time, duplicates, anomalies, etc.