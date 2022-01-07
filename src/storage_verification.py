import os

def verify_storage_files(files):

    not_stored_files=[file  for file in files if not os.path.exists(f'./data/{file.split(".")[0]}')]
    
    [print(f'Start process of columnar storage for file: {file}') if file in not_stored_files else print(f'File {file} already exists') for file in files ]

    return not_stored_files
