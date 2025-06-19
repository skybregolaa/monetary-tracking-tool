import logging
from pathlib import Path
from typing import Union
import yaml
import os
import pandas as pd


def load_yaml(yaml_path):
    with open(yaml_path, "r") as file:
        return yaml.safe_load(file)


def upload_dataframe(file_path, **kwargs):
    """
    Load a DataFrame from a file with support for various extensions.

    Parameters:
    - file_path (str): Path to the file.
    - **kwargs: Additional keyword arguments passed to the pandas reading functions.

    Returns:
    - pd.DataFrame: DataFrame loaded from the file.

    Raises:
    - ValueError: If the file extension is not supported.
    """
    # Extract the file extension
    extension = str(file_path).split(".")[-1].lower()

    # Load the DataFrame based on the file extension
    if extension == "csv":
        df = pd.read_csv(file_path, **kwargs)
    elif extension == "xlsx":
        # `sheet_name` can be provided in kwargs to specify which sheet to load
        sheet_name = kwargs.get("sheet_name", 0)  # Default to the first sheet
        df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
    elif extension == "pkl":
        df = pd.read_pickle(file_path)
    elif extension == "json":
        df = pd.read_json(file_path, **kwargs)
    elif extension == "tsv":
        df = pd.read_csv(file_path, sep="\t", **kwargs)
    elif extension == "parquet":
        df = pd.read_parquet(file_path, **kwargs)
    else:
        logging.error(
            f".{extension} extension not supported, available: .csv, .xlsx, .pkl, .json, .tsv"
        )

    return df


def save_dataframe(df: pd.DataFrame, file_path: Union[str, Path], **kwargs):
    """
    Save a DataFrame to a file with support for various formats.

    Parameters:
    - df (pd.DataFrame): The DataFrame to be saved.
    - file_path (str): Path to the file, including the desired extension.
    - **kwargs: Additional keyword arguments passed to the pandas writing functions.

    Raises:
    - ValueError: If the file extension is not supported.
    """
    # Make sure the directory exists, else create it
    ensure_directory_exists(Path(file_path).parent)

    # Extract the file extension
    extension = Path(file_path).suffix[1:].lower()

    # Save the DataFrame based on the file extension
    if extension == "csv":
        df.to_csv(file_path, **kwargs)
    elif extension == "xlsx":
        # `sheet_name` can be provided in kwargs to specify the sheet name
        sheet_name = kwargs.pop("sheet_name", "Sheet1")  # Default to 'Sheet1'
        df.to_excel(file_path, sheet_name=sheet_name, **kwargs)
    elif extension == "pkl":
        df.to_pickle(file_path)
    elif extension == "json":
        df.to_json(file_path, **kwargs)
    elif extension == "tsv":
        df.to_csv(file_path, sep="\t", **kwargs)
    elif extension == "parquet":
        df.to_parquet(file_path, **kwargs)
    else:
        logging.error(
            f".{extension} extension not supported, available: .csv, .xlsx, .pkl, .json, .tsv, .parquet"
        )
        raise ValueError(f".{extension} not supported")


def return_filenames_ext(folder_path: Union[str, Path], ext="sql"):
    """
    Return all files in a folder given an extension
    """
    # List to hold the paths of SQL files
    files_list = []

    # Iterate over all the files in the directory
    for file_name in os.listdir(folder_path):
        # Check if the file ends with '.ext'
        if file_name.endswith(f".{ext}"):
            # Add the full path of the file to the list
            full_path = os.path.join(folder_path, file_name)
            files_list.append(full_path)

    files_list.sort()

    return files_list


def return_string_versioning(max_v: int, version: int) -> list:
    assert version < max_v, logging.error(
        f"Version ({version}) greater than provided max ({max_v})"
    )
    num_digits = len(str(max_v))
    return f"{version:0{num_digits}}"


def ensure_directory_exists(dir_path: str):
    """
    Check if a directory exists, and create it if it does not.

    Parameters:
    ----------
    dir_path : str
        The path to the directory to check and potentially create.
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path



def save_plot(plt, filepath): 
    supported_extensions = ['png', 'pdf', 'ps', 'eps', 'svg', 'jpeg', 'jpg', 'tiff']
    filedir = os.path.dirname(filepath)
    ensure_directory_exists(filedir)
    extension = str(filepath).split('.')[-1]
    if extension not in supported_extensions:
        raise ValueError(f"Unsupported file extension '{extension}'. Supported extensions are: {supported_extensions}")
    plt.savefig(filepath, format=extension)