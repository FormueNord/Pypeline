import os
import sys
import importlib
import time


class Node:
    """class to run individual pipelines buy calling them when their triggers returns True"""

    def __init__(self):
        self._load_pipelines("Pipelines")
        self.monitor_pipelines()

    def monitor_pipelines(self):
        RUN_TIME = True

        while RUN_TIME:
            for pipeline in self.pipelines.values():
                pipeline.run() if pipeline.trigger() else None

        return
    
    def _load_pipelines(self,pipelines_folder):
        
        #get filename in pipelines_folder
        pipeline_files = os.listdir("Pipelines")
        pipeline_files = [file[:-3] for file in pipeline_files if file[-3:] == ".py" and file != "__init__.py"]
        
        #add pipeline_folder to path
        sys.path.append(pipelines_folder)

        #intialize each pipeline and add to self.pipelines
        self.pipelines = {}
        for module in pipeline_files:
            exec(f"class_obj = getattr(importlib.import_module('{module}'), '{module}')")
            exec(f"self.pipelines['{module}'] = class_obj()")
        return


if __name__ == "__main__":
    Node()