from common import Config, Timeslice, StageType
import json

pattern = "auto_load_schema"
config_path = f"./Config/{pattern}.yaml"
timeslice = Timeslice(day="*", month="*", year="*")
config = Config(timeslice=timeslice, config_path=config_path)
table_mapping = config.get_table_mapping(timeslice=timeslice, stage=StageType.raw, table="customers")






    