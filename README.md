# Databricks Patterns

This project contains various databricks patterns for operating and developing databricks platform patterns.

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
                autoload_raw_schema.py
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
|databricks/notebooks/bronze/autoload_raw_schema.py  | notebook to load a raw table using the config in `.pipelines`|
|databricks/notebooks/bronze/load_raw.py             | notebook to load all raw tables using the config in `.pipelines`, calls autoload_raw_schema.py|
|databricks/notebooks/etl                            | contains reusable python functions to load tables |
|pipelines/autoload_raw_schema.yaml                  | contains configuration to load the datalake house using autoloader |
|pipelines/tables.yaml                               | contains configuration that describes the tables and landing files to load |
|schema                                              | contains the spark schema for schema on read or schema hinting when loading the landing files |
|sql/control_header_footer                           | contains SQL table create scripts for tables we want to declare explicitly, in this case the audit tables we'll create |

## Executing

Follow these steps to execute the pipeline:

- clone this repo into databricks
- create and start a cluster using the latest cluster DBR LTS
- execute the notebook `./header_footer/databricks/notebooks/setup.py`, this will clear down if you've run it previously or copy the landing data into the correct location.
- execute the notebook `./header_footer/databricks/notebooks/bronze/load_raw.py `, this will load the data using autoloader into the datalake house and audit tables.
- execute the notebook `databricks/notebooks/checks.py`, this has simple select statements to explore the data that was just loaded into the raw and audit control tables.


