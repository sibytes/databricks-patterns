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


import os
from autobricks import Workspace

ROOT_DIR = os.getenv("ROOT_DIR")
WORKSPACE_ROOT = os.getenv("WORKSPACE_ROOT")
WORKSPACE_SUBDIRS:str = os.getenv("WORKSPACE_SUBDIRS")
NOTEBOOK_DIR:str = os.getenv("NOTEBOOK_DIR")

from_notebook_root = f"{ROOT_DIR}/{NOTEBOOK_DIR}/"
target_dir = f"/{WORKSPACE_ROOT}"
sub_folders = [f"/{d.strip()}" for d in WORKSPACE_SUBDIRS.split(",")]

for source_dir in sub_folders:

    # this will iterate all dirs in the from_notebook_root
    # if the root of the dir matches the source_dir then it
    # will deploy the folder otherwise it will skip it.
    # Deploy mode is PARENT it will copy the source dir 
    # into the target dir in the worksapce
    Workspace.workspace_import_dir(
        from_notebook_root=from_notebook_root,
        source_dir=source_dir,
        target_dir=target_dir,
        deploy_mode=Workspace.DeployMode.PARENT
    )






    