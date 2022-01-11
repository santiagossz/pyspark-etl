import os

def verify_storage_files(files):
    SPARK_WAREHOUSE = os.getenv('SPARK_WAREHOUSE')

    not_stored_files=[file  for file in files if not os.path.exists(f'{SPARK_WAREHOUSE}/{file.split(".")[0]}')]
    
    [print(f'File {file} already exists') for file in files if file not in not_stored_files]


    return not_stored_files
