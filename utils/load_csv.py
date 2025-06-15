from config import DATA_FOLDER
import os
import pandas as pd

def load_csv(file_name: str, **read_kwargs) -> pd.DataFrame:
    """
    Load a CSV file from the data folder.

    Args:
        file_name (str): The name of the CSV file to load.

    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
    file_path = os.path.join(DATA_FOLDER, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_name} does not exist in the data folder.")
    return pd.read_csv(file_path, **read_kwargs)