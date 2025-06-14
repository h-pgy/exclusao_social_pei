import os
import pandas as pd
from config import DATA_FOLDER

def save_csv(df:pd.DataFrame, filename):
    """
    Save data to a CSV file in the specified data folder.
    
    Parameters:
    - data: The data to be saved, should be a list of dictionaries.
    - filename: The name of the file to save the data to.
    """
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    
    filepath = os.path.join(DATA_FOLDER, filename)
    df.to_csv(filepath, index=False)

    print(f"Data saved to {filepath}")