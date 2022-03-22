import os
import sys

class Node:
    """
    Creates a class instance of Node which from each .py file in the pipelines_folder import a var with an equal name to the file.
    Each imported var must be an instance of the Pipeline class.
    The Node's monitor_pipelines method can be called to monitor each instance of pipelines trigger and activate its run method if conditions are met. 

    Args:
        pipelines_folder:  absolute path to a folder containing .py files with instances of the Pipeline class
    """

    def __init__(self,pipelines_folder):
        self._load_pipelines(pipelines_folder)
        self.pipelines_folder = pipelines_folder

    def monitor_pipelines(self):
        """
        Monitors loaded instances of Pipeline calling their trigger method and calls the run method if the trigger's conditions are met.
        """

        RUN_TIME = True

        while RUN_TIME:
            for pipeline in self.pipelines.values():
                destination = pipeline.trigger()
                if destination:
                    pipeline.run(destination)
        return
    
    def _load_pipelines(self,pipelines_folder):
        """
        Imports a var from each .py files with the same name as the .py file (except any "__init__.py"), where each var should be an instance of Pipeline.
        Each pipeline is appended to the self.pipelines dict, with the instance's name as key and the instance as the value.
        """

        #get filename in pipelines_folder
        pipeline_files = os.listdir("Pipelines")
        pipeline_files = [file[:-3] for file in pipeline_files if file[-3:] == ".py" and file != "__init__.py"]
        
        #add pipeline_folder to path
        sys.path.append(pipelines_folder)

        #intialize each pipeline and add to self.pipelines
        self.pipelines = {}
        for module in pipeline_files:
            exec(f"from {module} import {module}")
            #exec(f"class_obj = getattr(importlib.import_module('{module}'), '{module}')")
            exec(f"self.pipelines['{module}'] = {module}")
        return


if __name__ == "__main__":
    Node_instance = Node("Pipelines")
    Node_instance.monitor_pipelines()