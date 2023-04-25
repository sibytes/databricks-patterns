import os
from autobricks import Job

ROOT_DIR = os.getenv("ROOT_DIR")
SUB_DIR = os.getenv("SUB_DIR")


path = os.path.join(ROOT_DIR, SUB_DIR)

Job.job_import_jobs(path)