from scripts.utils import file_keys

def delete_used_file_keys():
    file_keys.delete_normalized_file_key()
    file_keys.set_next_untreated_file_key()