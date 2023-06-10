import os
from autobricks import Pipeline

ROOT_DIR = os.getenv("ROOT_DIR")
SUB_DIR = os.getenv("SUB_DIR")


path = os.path.join(ROOT_DIR, SUB_DIR)

Pipeline.pipeline_import_pipelines(path)