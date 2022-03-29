import os
import sys
import datetime
from ErrorAlerter import ErrorAlerter


class Node:
    """
    Creates a class instance of Node which from each .py file in the pipelines_folder import a var with an equal name to the file.
    Each imported var must be an instance of the Pipeline class.
    The Node's monitor_pipelines method can be called to monitor each instance of pipelines trigger and activate its run method if conditions are met. 

    Args:
        pipelines_folder:  Absolute path to a folder containing .py files with instances of the Pipeline class
        ErrorAlerter:  A class with functionality to send error alerts. Current implementation only supports emails.
    """

    def __init__(self,pipelines_folder,ErrorAlerter = ErrorAlerter):
        self._load_pipelines(pipelines_folder)
        self._create_error_message_tracker()
        self.ErrorAlerter = ErrorAlerter


    def monitor_pipelines(self):
        """
        Monitors loaded instances of Pipeline calling their trigger method and calls the run method if the trigger's conditions are met.
        """

        RUN_TIME = True

        while RUN_TIME:
            for pipeline_name in self.pipelines:
                destination = self.pipelines[pipeline_name].trigger()
                if destination:
                    #self.pipelines[pipeline_name].run(destination)
                    successful_run = self._pipeline_run_with_alert(pipeline_name,destination)
                    self._log_pipeline_run(pipeline_name) if successful_run else None
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
        
        #check if all pipelines have the needed credentials
        self._credentials_check()
        return

    def _credentials_check(self):
        
        print("Checking if the credentials for LoaderObj is stored locally for each Pipeline")
        for pipeline in self.pipelines.values():
            print(f"Checking credentials for: {pipeline._load_destination}")            
            pipeline._LoaderObj(pipeline._load_destination)

        print("Checking if credentials for the ErrorAlerter is stored locally")
        temp = ErrorAlerter("","","")

        print("All needed credentials are stored locally")
        return

    def _pipeline_run_with_alert(self,pipeline_name,destination):
        """
        Run the instance of Pipeline with pipeline_name and send error alert to recipients if it fails.
        To avoid spamming recipients a new error won't be sent after the first for five hours.

        Args:
            pipeline_name:  str matching a key in the self.pipelines dict
            destination:  the returned value from the Pipeline instances trigger_func. Exact type depends on the functions applied.
        """
        try:
            self.pipelines[pipeline_name].run(destination)
            return True
        except Exception as error:
            #if its been more than five hours since the last error was sent
            if self._error_message_tracker[pipeline_name] + \
            datetime.timedelta(hours = 5) < \
            datetime.datetime.now():
                #send error to people specified in Pipeline instance
                alerter = self.ErrorAlerter(
                    receivers = self.pipelines[pipeline_name]._error_notify_mails,
                    subject = f"Error in Pipeline {pipeline_name}",
                    warning_text = error
                )
                alerter.error_alert()
                #del instance to ensure that it closes down (maybe uneccessary)
                del alerter
                #set new datetime for last error sent
                self._error_message_tracker[pipeline_name] = datetime.datetime.now()
                print(f"An error message has been sent regarding error in {pipeline_name}")
            return False


    def _create_error_message_tracker(self):
        """
        Create a dict for each loaded instance of Pipeline with datetime(1,1,1).
        The dict is used to track when the last time an error message was sent to avoid spamming the recipients.
        """
        self._error_message_tracker = {}
        for key in self.pipelines.keys():
            self._error_message_tracker[key] = datetime.datetime(1,1,1)
        return


    def _log_pipeline_run(self,pipeline_name):
        str_to_write = f"{pipeline_name} ran successfully at {datetime.datetime.now()}"
        with open("Pipeline log.txt","a") as f:
            f.write(str_to_write + "\n")
        return
            

def main():
    Node_instance = Node("Pipelines")
    Node_instance.monitor_pipelines()

if __name__ == "__main__":
    main()