# from dbxconfig import Config, Timeslice, StageType, Read, DeltaLake


# pattern = "auto_load_schema"
# config_path = f"./Databricks/Config/{pattern}.yaml"
# timeslice = Timeslice(day="*", month="*", year="*")
# config = Config(config_path=config_path)
# table_mapping = config.get_table_mapping(timeslice=timeslice, stage=StageType.raw, table="customers")

# print(table_mapping)

# source:Read = table_mapping.source["customer_details_1"]
# destination:DeltaLake = table_mapping.destination
# config.link_checkpoint(source=source, destination=destination)




    