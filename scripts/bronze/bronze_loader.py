"""##################################################################
# Script        : FileLoader.py
# Description   : Loads flat files from a configured directory into
#                 Snowflake tables. Each file name is assumed to match a target
#                 table name in bronze schema. The script
#                 truncates the target table before loading fresh data.
#####################################################################"""


import os
import configparser
from typing import Dict, Tuple

from modules.SnowflakeDatabase import SnowflakeConnection


def read_config(config_path: str) -> Tuple[Dict[str, str], str, str]:
    """
    Reads configuration values from the given config file.

    Args:
        config_path (str): Path to the environment configuration file.

    Returns:
        Tuple containing:
            - Snowflake connection parameters as a dictionary
            - Dataset directory path
            - File format of the dataset

    Raises:
        FileNotFoundError: If the configuration file is not found.
        KeyError: If required sections or keys are missing.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path)

    # Read Snowflake connection parameters
    parameters: Dict[str, str] = dict(config["Parameters"])

    # Read dataset configuration
    dataset_path: str = config["Dataset"]["path"]
    file_format: str = config["Dataset"]["file_format"]

    return parameters, dataset_path, file_format


def process_files(
    connection: SnowflakeConnection,
    dataset_path: str,
    file_format: str
) -> None:
    """
    Truncates target tables and loads data from files into Snowflake.

    Each file name (without extension) is assumed to match a target table name.

    Args:
        connection (SnowflakeConnection): Active Snowflake connection object.
        dataset_path (str): Directory containing dataset files.
        file_format (str): File format used for loading (e.g., CSV, JSON).

    Returns:
        None
    """
    for file_name in os.listdir(dataset_path):

        # Skip hidden/system files
        if file_name.startswith("."):
            continue

        table_name: str = os.path.splitext(file_name)[0]

        # Clear existing data before load
        connection.truncate_table(table_name)

        # Load data from file into Snowflake table
        connection.load_from_file(
            table_name=table_name,
            file_path=dataset_path,
            file_format=file_format
        )


def main() -> None:
    """
    Main body data loading script.
    """
    CONFIG_PATH = "scripts/config/env.cfg"

    # Read configuration
    parameters, dataset_path, file_format = read_config(CONFIG_PATH)

    # Initialize Snowflake connection
    connection = SnowflakeConnection(parameters)

    # Process and load dataset files
    process_files(connection, dataset_path, file_format)


if __name__ == "__main__":
    main()
