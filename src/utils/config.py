from pathlib import Path
from typing import Optional
from pydantic.v1 import BaseModel
from datetime import datetime

import yaml


project_root = Path(__file__).parents[2]
today = datetime.now()


def load_yaml_config(yaml_path: Optional[Path] = None):
    if yaml_path and yaml_path.exists():
        with open(yaml_path, "r") as f:
            return yaml.safe_load(f)
    return {}


class _Directories(BaseModel):
    project_dir = project_root
    src_dir = project_dir / "src"
    sql_scripts_dir = src_dir / "sql_scripts"
    data_dir = project_dir / "data"
    config_dir = project_dir / "config"
    data_input_dir = data_dir / "input"
    data_output_dir = data_dir / "output"
    plot_directories = data_dir / "plots"
    bq_variables = config_dir / "env_variables.yaml"


class _Constants(BaseModel):
    formatted_date = today.strftime("%Y%m%d")



class Config(BaseModel):
    DIRECTORIES = _Directories()
    CONSTANTS = _Constants()


local_config = load_yaml_config(project_root / "config/local_config.yaml")

config = Config(**local_config)
