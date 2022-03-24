import sys
parent_dir = "\\".join(__file__.split("\\")[:-2])
sys.path.append(parent_dir)
from Pipeline import Pipeline
from trigger_functions import folder_monitor_constructor
from extractor_functions import csv_extractor_constructor

class nothing:
    pass

test1 = Pipeline(
    folder_monitor_constructor("C:\\Users\\ChrisAlbertsen\\Desktop\\Pipeline\\Pipelines\\files", in_name_criteria = ".csv"),
    csv_extractor_constructor(csv_seperator = ","),
    load_destination = {
        "server": "formuenord.database.windows.net",
        "database": "formuenordDB",
        "table": "testTable"},
        error_notify_mails = "ca@formuenord.dk,mk@formuenord.dk"
        )