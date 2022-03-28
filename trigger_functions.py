import os

def function_constructor(func):
    """
    Decorator used to construct a function
    """
    def wrapper(folder_path, **kwargs):
        return lambda: func(folder_path, **kwargs)
    return wrapper

@function_constructor
def folder_monitor_constructor(folder_path, in_name_criteria = None):
    """
    Constructs a function that monitors folder content and returns values based on content and set criteria

    Args:
        folder:  specify path to monitored folder
        in_name_criteria:  filter out any files in folder not containing the criteria in name

    Returns:
        List of file names in folder containing criteria (all files if no criteria is set) 
    """
    files_in_folder = os.listdir(folder_path)
    if in_name_criteria:
        trigger_files = [file for file in files_in_folder if in_name_criteria in file]
        trigger_files = [os.path.join(folder_path,file) for file in trigger_files]
        return trigger_files
    return [os.path.join(folder_path,file) for file in files_in_folder]