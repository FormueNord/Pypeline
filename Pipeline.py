from AzureLoader import AzureLoader 


class Pipeline:
   
    def __init__(self, extractor_func , load_destination: dict, transformer_func = lambda x: x, trigger_func = lambda: False, run_func = None, 
    loaderObj = AzureLoader, check_func = lambda: None):
        self._extractor_func = extractor_func
        self.load_destination = load_destination
        self._transformer_func = transformer_func
        self._run_func = run_func
        self._trigger_func = trigger_func
        self._loaderObj = AzureLoader
        self._check_func = check_func

    def extract(self):
        """
        Initializes extract function.
        Adding a function called extract to the child class is necessary, otherwise no data is extracted to the pipeline.
        """
        self.data = self._extractor_func()
        return
    
    def transform(self):
        """
        Initializes empty transform function. Add further functionality in child function if relevant.
        """
        self.data = self._transformer_func(self.data)
        return
    
    def load(self):
        """
        Loads data to an Azure SQL database
        """
        cnxion = self._loaderObj(self.load_destination)
        cnxion.insert_to(self.data,self.load_destination["Table"])
        return

    def trigger(self):
        """
        Run _trigger_func
        """
        return self._trigger_func()

    def check(self):
        """
        Run function to check the data uploaded from the Pipeline
        """
        self._check_func()
        return

    def run(self):

        #if user defined function exists run it
        if self._run_func:
            self._run_func()
        else:
            self.extract()
            self.transform()
            #self.load()
            self.check()

        #clean pipeline
        if self.data:
            del self.data
        return
    
