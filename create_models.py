import os
import glob
import subprocess
import json
import logging
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def find_models_folder(root_path):
    """Find the 'models' folder within the root_path using glob."""
    search_pattern = os.path.join(root_path, '**', 'models')
    models_folders = glob.glob(search_pattern, recursive=True)
    if models_folders:
        return models_folders[0]  # Return the first found models folder
    return None

def generate_model_yaml(folder_path):
    """Generate model YAML files using dbt run-operation."""
    models_folder = find_models_folder(folder_path)
    
    if models_folder is None:
        raise FileNotFoundError(f"The 'models' folder does not exist within {folder_path}")

    # Get the path of SQL files in the models folder
    sql_files = glob.glob(os.path.join(models_folder, '*.sql'))
    
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in the 'models' folder at {models_folder}")

    # Extract filenames without extension
    file_names = [os.path.splitext(os.path.basename(file))[0] for file in sql_files]

    # Prepare the dbt command
    args = json.dumps({"model_names": file_names})
    command = f"dbt run-operation generate_model_yaml --args '{args}'"

    # Change the working directory to one level above 'models'
    os.chdir(os.path.dirname(models_folder))

    # Execute the dbt command and capture the output
    try:
        result = subprocess.run(command, shell=True, text=True, capture_output=True, check=True)
        output = result.stdout
        logging.info("dbt command executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to execute dbt command: {e.stderr}")
        raise

    # Extract text starting from "version: 2"
    match = re.search(r'(version: 2.*)', output, re.DOTALL)
    if match:
        filtered_output = match.group(1)
    else:
        logging.warning("No 'version: 2' section found in the dbt output.")
        filtered_output = ""

    # Write the filtered output to models.yml
    with open(os.path.join(models_folder, 'models.yml'), 'w') as file:
        file.write(filtered_output)
        logging.info(f"Models YAML written to {os.path.join(models_folder, 'models.yml')}")

# Example usage:
folder_path = "/Users/dougstrouth/Documents/GitHub/nfl_data_prep"
generate_model_yaml(folder_path)