from AzureLoader import AzureLoader 
from ErrorAlerter import ErrorAlerter
import os
import shutil
import datetime

class Pipeline:
    """
    The class creates a data pipeline using the supplied functions, and allows for further functionality using the optional args.
    The instance of Pipeline is supplied to the Node class which then runs the run method (or optionally run_func), if the 
    trigger_func returns a value than can be evaluated as True (True, non-empty list or likewise).

    The default run method goes: extract -> transform -> load -> check -> clean

    Args:
        trigger_func:  Function taking no args and returning a destination (e.g. absolute path or https) for the extract_func or alternatively nothing.
                       Function is prompted by Node, and if anything evaluated as True is returned, the self.run method will run with the trigger_func return as arg.
                       Condition can be used to conditionally select files, set timer for running etc.
        
        extractor_func:  Function taking trigger_func's return as arg. Should return a pandas dataframe or series containing data.

        load_destination:  Dictionary containing 'server','database' and 'table' keys with values referencing the load destination.

        error_notify_mails:  Str with mails of people who are to be notified if an error occurs in the Pipeline

        transformer_func (optional):  Function taking a pandas dataframe or series as arg. Transforms the arg input as specified.

        check_func (optional):  Function running any checks defined in the check_func.

        run_func (optional):  Function changing the default run workflow.

        cleaning (optional):  Value defining what cleaning up to do.

        interval (optional): datetime.timedelta defining in what intervals the trigger should be called.

        LoaderObj (optional):  A class used to load the Pipeline's data a specified destination. Default is a class used to load to Azure, but can be changed,
                               if one wishes to load the AWS or any alternative specification. See code for context.
    
    Returns:
        An instance of the Pipeline object.
    """


    def __init__(self, trigger_func, extractor_func, load_destination: dict, error_notify_mails: str, transformer_func = lambda x: x,
        check_func = lambda: None, run_func = None, cleaning: str = "move", timer: dict = None, LoaderObj = AzureLoader):
        self._trigger_func = trigger_func
        self._extractor_func = extractor_func
        self._load_destination = load_destination
        self._transformer_func = transformer_func
        self._run_func = run_func
        self._LoaderObj = LoaderObj
        self._check_func = check_func
        self._error_notify_mails = error_notify_mails
        self.cleaning = cleaning
        self.timer = timer


    def trigger(self, time_delta : datetime.timedelta = None):
        """
        Runs _trigger_func

        Returns:
            Reference to target extraction point for data e.g. absolute path, https or other extraction target.
            Returned val(s) is used by self.extract for extraction targets.
        """
        return self._trigger_func()


    def extract(self,trigger_result):
        """
        Runs _extract_func

        Args:
            trigger_result:  Reference to target extraction point for data i.e. absolute path, https or other extraction target.
                             Arg depends on the trigger_func and extractor_func used to initialize the instance of Pipeline
        """
        self.data = self._extractor_func(trigger_result)
        return
    

    def transform(self):
        """
        Applies any specified transformations to self.data.
        If no transformer_func is specified when initializing the instance of Pipeline, the function will return self.data unchanged.

        Returns:
            Pandas dataframe
        """
        self.data = self._transformer_func(self.data)
        return
    

    def load(self):
        """
        Loads data to target using the self._loaderObj (default is Azure SQL Database).
        Target Azure server, database and table is specified in the dict _load_destination.
        """
        cnxion = self._LoaderObj(self._load_destination)
        cnxion.insert(self.data,self._load_destination["table"])
        return


    def check(self):
        """
        Run function to check the data uploaded from the Pipeline.
        If not _check_func is supplied when initializing the Pipeline instance the check simply passes.
        """
        self._check_func()
        return
    

    def clean(self, trigger_result):
        """
        Cleans the pipeline making it ready for a new run workflow.

        Args:
            trigger_result:  Reference to target extraction point for data i.e. absolute path, https or other extraction target.
                             Arg depends on the trigger_func and extractor_func used to initialize the instance of Pipeline
        """
        #do something with the uploaded file
        if isinstance(trigger_result,str): 
            for src_path in trigger_result:
                if os.path.exists(src_path):
                    if self.cleaning == "delete":
                        os.remove(src_path)
                    if self.cleaning == "move":
                        self._move_and_mkdir(src_path)
                    else:
                        raise Exception("defined cleaning variable doesn't match any of the implemented operations")

        #delete self.data if it exists
        if "data" in dir(self):
            del self.data
        return

    
    def _move_and_mkdir(self,src_path):
        dst_folder = os.path.join("\\".join(src_path.split("\\")[:-1]),"uploaded")
        if not os.path.exists(dst_folder):
            os.mkdir(dst_folder)
        file_name = src_path.split("\\")[-1]
        shutil.move(src_path,os.path.join(dst_folder,file_name))
        return


    def run(self,trigger_result):
        """
        Runs the default workflow (or _run_func workflow if specified) extract, transform, load, check, clean.

        Args:
            trigger_result:  Reference to target extraction point for data i.e. absolute path, https or other extraction target.
                             Arg depends on the trigger_func and extractor_func used to initialize the instance of Pipeline
        """
        #if user defined function exists run it
        if self._run_func:
            self._run_func(trigger_result)
        else:
            self.extract(trigger_result)
            self.transform()
            self.load()
            self.check()
            self.clean(trigger_result)
        return True