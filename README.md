# Databricks Patterns

This project contains various databricks patterns for operating and developing databricks solutions.

# Data Engineering Patterns

Patterns for data engineers to load data into the data lakehouse.

## Customer Details - Header Footer

This example pattern shows data pipelines loading a difficult file provision to from landing to raw using autoloader.

The data files can be seen here:

```
./data/landing/customer_details
```

The files are difficult to deal with because they have no column name headers and have a semi structured header and footer that is different to the rest of the file.

In order to load the files we're using a www.yetl.io that is a configuration framework built for spark.

The project structure is as follows:

```
./header_footer
    header_footer.yaml
    logging.yaml
    databricks
        notebooks
            bronze
                etl
                checks.py
                load_table.py
                raw_load.py
            setup.py
        workflows
            autoloader-raw-schema.json
    pipelines
        autoload_raw_schema.yaml
        tables.yaml
    schema
        customer_details_1.yaml
        customer_details_2.yaml
    sql/control_header_footer
        header_footer.sql
        raw_audit.sql
```

This structure is described as follows from `./header_footer`:

| path | description |
|-|-|
|header_audit.yaml                                   | describes to yetl the project structure and project name |
|databricks                                          | contains all databricks assets |
|databricks/notebooks/setup.py                       | notebook to clear down and copy the demo data into landing |
|databricks/notebooks/checks.py                      | notebook to explore the data after loading |
|databricks/notebooks/bronze                         | contains notebooks for loading bronze or raw |
|databricks/notebooks/bronze/load_table.py           | notebook to load a raw table using the config in `.pipelines`|
|databricks/notebooks/bronze/load_raw.py             | notebook to load all raw tables using the config in `.pipelines`, calls autoload_raw_schema.py|
|databricks/notebooks/etl                            | contains reusable python functions to load tables |
|databricks/workflow/autoloader-raw-schema           | json definition of databricks workflow |
|pipelines/autoloader.yaml                           | contains a databricks workflow definition for executing the raw load using autoloader |
|pipelines/batch.yaml                                | contains a databricks workflow definition for executing the raw load using a batch pattern |
|pipelines/tables.yaml                               | contains configuration that describes the tables and landing files to load |
|schema                                              | contains the spark schema for schema on read or schema hinting when loading the landing files |
|sql/control_header_footer                           | contains SQL table create scripts for tables we want to declare explicitly, in this case the audit tables we'll create |

## Executing

Follow these steps to execute the pipeline:

- clone this repo into databricks
- create and start a cluster using the latest cluster DBR LTS
- execute the notebook `./header_footer/databricks/notebooks/setup.py`, this will clear down if you've run it previously or copy the landing data into the correct location.
- execute the notebook `./header_footer/databricks/notebooks/bronze/load_raw.py` with the default parameters, this will load the data using autoloader into the datalake house and audit tables. Note the following parameters:

| name | default | type | description |
|-|-|-|-|
| process_id    | long | -1   | unique process id stamped against the tables, when scheduled can be the {{job_id}} |
| max_parallel  | int  | 4    | maximum number of parallel tables to load using the notebook |
| timeout       | int  | 3600 | execution timeout in seconds |
| process_group | int  | 1    | tables with the customer property process_group will be executed. The framework allows custom properties to be set on tables. You can use them as a lookup filter or as properties to use in the ETL process. This one is used to filter the tables to execute. Assigning a process_group properties would allow us to break up large amounts of tables across different job clusters without being limited to a single table perd job cluster.
| load_type     | string: autoloader, batch | autoloader | The load pattern you want to use, there are 2 load patterns in this project. In `./pipelines/header_footer` there is `autoloader.yaml` and `batch.yaml` declared, use the file name of the pattern without the extension in parameter to switch between them.
| timeslice | string YYYY-mm-dd | * | when using the batch load pattern a timeslice can be provided to indicate which timeslice to load from landing. * will load everything. Other examples are 2023-01-01, 2023-01-\*, 2023-\*-\* |

- execute the notebook `./header_footer/databricks/notebooks/bronze/load_raw.py` this time setting `process_group = 2` and `process_id = -2`. This will load the tables with the custom property `process_group = 2` defined in `./pipelines/header_footer/pipelines/tables.yaml`. The data will be stamped against with `process_id = -2`. On manual runs I tend to use -ve number because they can easily be distinguised from automated runs that are +ver numbers.

- execute the notebook `databricks/notebooks/checks.py`, this has simple select statements to explore the data that was just loaded into the raw and audit control tables.
- execute the notebook `./header_footer/databricks/notebooks/setup.py`, to reset the landing data and clean up delta cataolog and file tables.


