import AzureLoader


class Pipeline:
   
    def __init__(self,extractor: function,load_to: function, transformer = lambda x: x, triggerFunc = lambda: False):
        self.extractor = extractor
        self.load_to = load_to
        self.transformer = transformer
        self.trigger = triggerFunc

    def extract(self):
        """
        Initializes extract function.
        Adding a function called extract to the child class is necessary, otherwise no data is extracted to the pipeline.
        """
        self.data = self.extractor()
        return
    
    def transform(self):
        """
        Initializes empty transform function. Add further functionality in child function if relevant.
        """
        self.data = self.transformer(self.data)
        return
    
    def load(self):
        """
        Loads data to an Azure SQL database
        """
        cnxion = AzureLoader()
        cnxion.insert_to(self.data,self.load_to)
        return
