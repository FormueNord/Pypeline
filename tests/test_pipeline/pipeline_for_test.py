from Pypeline.Pipeline import Pipeline
import os

test_file_path = os.path.join("\\".join(__file__.split("\\")[0:-1]),"monitor_pipelines_test_file.txt")

def trigger_func():
    return True

def extractor_func(trigger_result):
    return [1,2,3,4,5,6,7,8,9]

def transformer_func(data):
    data.append(10)
    return data

def run_func(self):
    data = extractor_func(trigger_func())
    data = transformer_func(data)
    with open(test_file_path, "w") as f:
        f.write(str(data))

pipeline_for_test = Pipeline(trigger_func = trigger_func,
                            extractor_func = extractor_func,
                            transformer_func = transformer_func,
                            run_func = run_func,
                            load_destination= {"this is not needed": None},
                            error_notify_mails="chris.albertsen90@gmail.com")