# Items in the data directory
- *01 - Medallion.ipynb*
- *02 - Profiling & Visualization.ipynb*
- *03 - Metadata Tracking & Code Lineage.ipynb*
- *04 - Data Contracts.ipynb*
- *05 - API.ipynb*
- *06 - Time Series Modeling.ipynb*
- *logger.py*

## 01 - Medallion.ipynb
The Medallion notebook is used to create the bronze, silver, and gold layers of the medallion architecture.
The bronze layer is created by reading the raw data from the [data/rawdata](../data/rawdata/) directory and saving it as a delta table.
The silver layer is created by performing data integrity checks on the bronze layer and saving the results as a delta table.
The gold layer is created by joining the data from the bronze and silver layers and saving it as a delta table.
Statistics about the ingestion process are saved in the *statistics.json* file located in [docs/medallion](../docs/medallion/).
Notebook can be re-run as needed.

## 02 - Profiling & Visualization.ipynb
The Profiling & Visualization notebook is used to generate profile reports and visualizations for the data.
The profile reports are generated using the pandas profiling library and are saved in the [example/profile](../example/profile/) directory.
The visualization is done using *Power BI* Desktop application and the report is saved in the [example/visualization](../example/visualization/) directory.
The Power BI report is used to visualize the data from the gold layer of the medallion architecture.
Visualizations are also done using the matplotlib library in the notebook.
Notebook can be re-run as needed.

## 03 - Metadata Tracking & Code Lineage.ipynb
The Metadata Tracking & Code Lineage notebook is used to generate metadata for the data used in the project.
The metadata is saved in the [docs/metadata](../docs/metadata/) directory.
The metadata is saved in the form of delta tables and contains information about the data lineage from bronze to silver to gold layer.
Separate metadata is stored for the golden layer and the data lineage.
Notebook can be re-run as needed.

## 04 - Data Contracts.ipynb
The Data Contracts notebook is used to generate data contracts for the data used in the project.
The data contracts are saved in the [docs/contracts](../docs/contracts/) directory.
The contracts are saved in the JSON format and there is a contract for each data file used for bronze, silver, and gold layers.
Notebook can be re-run as needed.

## 05 - API.ipynb
The API notebook is used to create a RESTful API for the project.
FastAPI is used to create the API and the API is used to serve the data from the gold layer of the medallion architecture.
API is published using ngrok and the data can be accessed using the url provided by ngrok.
Endpoints for the API are:
1. /golden_data - to get the data from the gold layer - has filters for the data (company, region, date)
2. /download_golden_data - to download the data from the gold layer
Notebook can be re-run as needed.

## 06 - Time Series Modeling.ipynb
The Time Series Modeling notebook is used to train an ARIMA model on the data from the gold layer of the medallion architecture.
The ARIMA model is used to predict the future values of the time series data, specifically the future daily number of sales of cars.
The model is saved in the arima_model.zip file located in the [misc](../misc/) directory.
The model is saved in the pickle format inside of the zipped format.
Notebook can be re-run as needed.

## logger.py
The logger.py file contains the logger configuration for the project.
The logger is used to log information about the project, such as data ingestion, data processing, and data validation.
Every notebook from the *code* directory has its own log file in the [test/logs](../test/logs/) directory.
The logs are used to store the output of the notebooks.
They document the following structure:
- The name of the notebook (determines which file the log message belongs to)
- Level of log (INFO, WARNING, ERROR)
- The calling function
- The message
- The time of the log message