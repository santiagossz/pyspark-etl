import os

from os.path import abspath

def verify_storage_files(files):

    not_stored_files=[file  for file in files if not os.path.exists(f'./data/spark-warehouse/{file.split(".")[0]}')]
    
    [print(f'File {file} already exists') for file in files if file not in not_stored_files]


    return not_stored_files
