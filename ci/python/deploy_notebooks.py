import os
from autobricks import Workspace

FROM_ROOT = os.getenv("FROM_ROOT")
FROM_DIR = os.getenv("FROM_DIR")
TO_WORKSPACE_DIR:str = os.getenv("TO_WORKSPACE_DIR")

from_path = f"{FROM_ROOT}/{FROM_DIR}"
to_path = f"/{TO_WORKSPACE_DIR}"

Workspace.workspace_import_dir(
    from_path=from_path,
    to_path=to_path,
    # sub_dirs=["databricks","pipelines"]
)