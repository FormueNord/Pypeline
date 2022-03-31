import os
import sys
import datetime
import time
from ErrorAlerter import ErrorAlerter
from RunTracker import RunTracker

class Node:
    """
    Creates a class instance of Node which from each .py file in the pipelines_folder import a var with an equal name to the file.
    Each imported var must be an instance of the Pipeline class.
    The Node's monitor_pipelines method can be called to monitor each instance of pipelines trigger and activate its run method if conditions are met. 

    Args:
        pipelines_folder:  Absolute path to a folder containing .py files with instances of the Pipeline class
        ErrorAlerter:  A class with functionality to send error alerts. Current implementation only supports emails.
    """

    def __init__(self,pipelines_folder,ErrorAlerter = ErrorAlerter, RunTracker = RunTracker):
        self._load_pipelines(pipelines_folder)
        self.tracker = RunTracker(self.pipelines.keys())
        self.ErrorAlerter = ErrorAlerter


    def monitor_pipelines(self):
        """
        Monitors loaded instances of Pipeline calling their trigger method and calls the run method if the trigger's conditions are met.
        """

        RUN_TIME = True

        while RUN_TIME:
            for pipeline_name, pipeline_instance in self.pipelines.items():
                destination = self._run_with_alert(pipeline_name,lambda: self._trigger_with_timer(pipeline_name,pipeline_instance))
                if destination:
                    #self.pipelines[pipeline_name].run(destination)
                    successful_run = self._run_with_alert(pipeline_name,lambda: self.pipeline_instance.run(destination))
                    self._log_pipeline_run(pipeline_name) if successful_run else None
            time.sleep(10)
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
            exec(f"self.pipelines['{module}'] = {module}")
        
        #check if all pipelines have the needed credentials
        self._credentials_check()
        return
    

    def _trigger_with_timer(self,pipeline_name,pipeline_instance):
        #if pipeline has a defined interval check if the next trigger run is overdue
        if pipeline_instance.interval:
            last_run = self.tracker.tracking_data[pipeline_name]["last trigger"]
            now = datetime.datetime.now()
            if last_run + pipeline_instance.interval < now:
                trigger_result = pipeline_instance.trigger()
                self.tracker.update(pipeline_name,"last trigger",now)
                return trigger_result
            else:
                return
        return pipeline_instance.trigger()


    def _credentials_check(self):
        print("Checking if the credentials for LoaderObj is stored locally for each Pipeline")
        load_destinations = []
        for pipeline in self.pipelines.values():
            load_destinations.append({k: pipeline._load_destination[k] for k in pipeline._load_destination.keys() - {"table"}})
        load_destinations = [dict(y) for y in set(tuple(x.items()) for x in load_destinations)]
        for load_destination in load_destinations:
            print(f"Checking credentials for: {load_destination}")            
            pipeline._LoaderObj(load_destination, testing_credentials = True)

        print("Checking if credentials for the ErrorAlerter is stored locally")
        temp = ErrorAlerter("","","")

        print("Credentials are stored locally")
        return


    def _run_with_alert(self,pipeline_name,func):
        """
        Run the instance of Pipeline with pipeline_name and send error alert to recipients if it fails.
        To avoid spamming recipients a new error won't be sent after the first for five hours.

        Args:
            pipeline_name:  str matching a key in the self.pipelines dict
            destination:  the returned value from the Pipeline instances trigger_func. Exact type depends on the functions applied.
        """
        try:
            func()
            return True
        except Exception as error:
            #if its been more than five hours since the last error was sent
            now = datetime.datetime.now()
            if self.tracker.tracking_data[pipeline_name]["last error"] + \
            datetime.timedelta(hours = 5) < now:
                #send error to people specified in Pipeline instance
                alerter = self.ErrorAlerter(
                    receivers = self.pipelines[pipeline_name]._error_notify_mails,
                    subject = f"Error in Pipeline {pipeline_name}",
                    warning_text = error
                )
                alerter.error_alert()
                self.tracker.update(pipeline_name,"last error",now)
                #del instance to ensure that it closes down (maybe uneccessary)
                del alerter
                #set new datetime for last error sent
                print(f"An error message has been sent regarding error in {pipeline_name}")
            return False
        

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