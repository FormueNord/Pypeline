import pandas as pd

#delete when warning message is fixed in Pandas
import warnings
warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")


def function_constructor(func):
    """
    Decorator used to construct a function
    """
    def wrapper(*args, **kwargs):
        return lambda file_paths: func(file_paths, **kwargs)
    return wrapper

def all_files_in_folder_deco(func):
    """
    Decorator to repeat func for all files in folder

    Returns:
        Appended dataframe containing data of all files in file_paths
    """
    def wrapper(file_paths, **kwargs):
        #avoid repeating args 
        func_readied = lambda x: func(x,**kwargs)

        #handle list of file paths
        if isinstance(file_paths,list):
            if len(file_paths) > 1:
                data = func_readied(file_paths[0])
                for file_path in file_paths[1:]:
                    data = data.append(func_readied(file_path))
                return data
            else:
                #transform list to str if list length <= 1
                file_paths = file_paths[0]

        return func_readied(file_paths)
    return wrapper

@function_constructor
@all_files_in_folder_deco
def csv_extractor_constructor(file_path: str, csv_seperator = ";", csv_encoding = "UTF-8", header = "infer"):
    """
    Constructs a .csv reading function. 
    The constructed function read all files in file_paths (str or lst).

    Args:
        file_paths:  will be sourced from the Pipelines trigger function
        csv_seperator:  can be specified in the constructor to change the default seperator used
        csv_encoding:  can be specified in the constructor to change the default encoding

    Returns:
        Dataframe appended from all files in files_path (str or lst)
    """
    data = pd.read_csv(file_path,sep = csv_seperator, encoding = csv_encoding, header = header)
    return data


