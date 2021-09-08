import os
FILE_PATH = './dags/tmp'
UNTREATED_FILE=FILE_PATH+'/untreated_file_keys.txt'
NORMALIZED_FILE=FILE_PATH+'/normalized_file_keys.txt'

def get_next_untreated_file_key():
    with open(UNTREATED_FILE,'r') as file:
        for line in file:
            return line
        else:
            return None

def set_next_untreated_file_key():
    file_keys = []
    with open(UNTREATED_FILE, 'r') as file:
        for line in file:
            file_keys.append(line)
    if len(file_keys) > 0:
        file_keys.pop(0)
    with open(UNTREATED_FILE, 'w') as file:
        file.writelines(file_keys)

def get_next_normalized_file_key():
    try:
        with open(NORMALIZED_FILE,'r') as file:
            for line in file:
                return line
            else:
                return None
    except: 
        return None

def set_next_normalized_file_key(normalized_file_key):
    with open(NORMALIZED_FILE,'w+') as file:
        file.write(normalized_file_key)

def delete_normalized_file_key():
    os.remove(NORMALIZED_FILE)