import AzureLoader


class Pipeline:
   
    def __init__(self,extractor: function,load_to: function, transformer = lambda x: x, triggerFunc = lambda: False, run_func = lambda: False):
        self._extractor = extractor
        self._load_to = load_to
        self._transformer = transformer
        self._run_func = run_func
        self.trigger = triggerFunc

    def _extract(self):
        """
        Initializes extract function.
        Adding a function called extract to the child class is necessary, otherwise no data is extracted to the pipeline.
        """
        self.data = self._extractor()
        return
    
    def _transform(self):
        """
        Initializes empty transform function. Add further functionality in child function if relevant.
        """
        self.data = self._transformer(self.data)
        return
    
    def _load(self):
        """
        Loads data to an Azure SQL database
        """
        cnxion = AzureLoader()
        cnxion.insert_to(self.data,self._load_to)
        return

    def _check(self):
        """
        Run function to check the data uploaded from the Pipeline
        """
        return

    

    def run(self):
        if not self._run_func: 
            self.extract()
            self.transform()
            self.load()
            self.check()
        else:
            self._run_func()

        del self.data
        return
    
