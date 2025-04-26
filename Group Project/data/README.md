# Items in the data directory
- *rawdata* subdirectory
- *processed_data* subdirectory

## rawdata subdirectory
The rawdata directory contains the raw data used in the project. 
*car_sales_data* and *customers_data* are stored in the CSV format, while *companies_data* is stored in the JSON format.
This data is used to create the bronze layer of the medallion architecture.

## processed_data subdirectory
The processed_data directory contains the results of the *data_integrity_check* function from the [Medallion](../code/01%20-%20Medallion.ipynb) notebook located in the code directory.
The data_integrity_check function checks the data for anomalies and duplicates and saves the results in the processed_data directory.
This is used for automatic data validation and quality assurance.
We still used separate processing methods, because anomalies need to be defined based on the data, but this automatic validation can be used as a first step in the data validation process.