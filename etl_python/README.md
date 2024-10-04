# ETL Project with Bonobo Framework & Pandas
Project to build ETL pipeline using Pandas, PostgreSQL and Bonobo framework with Unit Tests added.

## Features

- Extract: Fetches files from data/movies.csv (or) data/movies.json, and credits.csv
           and create dataframe for processing.

- Transforms: Added transformations such as removing duplicates, get crew size, get names, etc.
              from fetched datasets

- Load: Load data to postgres database after joining two datasets (based on id),
        retrieves the highest rating directors' name.

- Utilized bonobo framework to generate simple workflow.  
====================================================================================
## Execute ETL pipeline:
1. Ensure Python version: python3.8 or higher
2. Install packages required
````
    pip install -r requirements.txt
````
3. Pull down database from Docker Hub & run local postgesql container

```
    docker pull postgres:14.11
    docker run --name local-pg -e POSTGRES_PASSWORD=pwd -e POSTGRES_USER=postgres -p 5432:5432 -d postgres   
```

4. From the terminal navigate to ETL_Python folder to run module.

````
python3 main.py -e <file_type> -e <file_name>
Both file_type, file_name are optional environment variables
````

### Example:
Run default with csv file on /data folder or alternatively specify json file on /data
````
python3 main.py
(or)
python3 main.py -e file_type=json -e file_name=movies.json
````

## Logging and database connection class
Logging module added so that the logs are sent to both console & log file
sample log file : Etl-run.log

Database connection class:
Connects to database from pool of connections with values provided from db_config.json file

===========================================================================================

## Unit tests:
/tests directory has required unit tests for transformation, loading, and extraction

````
command : cd /etl_python/tests && pytest .
````
