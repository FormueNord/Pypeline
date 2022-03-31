import datetime
import os
import pickle

class RunTracker:

    _tracking_file_path = os.path.join(os.path.dirname(__file__),"Tracking data.pickle")

    _template_subdict = {
        "last trigger": datetime.datetime(1,1,1),
        "last error": datetime.datetime(1,1,1),
    }

    def __init__(self,pipeline_names: list[str]):
        self.tracking_data = self._load_tracking_data(pipeline_names)


    def _load_tracking_data(self,pipeline_names):
        try:
            with open(self._tracking_file_path, "rb") as f:
                self.tracking_data = pickle.load(f)
            self._check_loaded_data(pipeline_names)
            return self.tracking_data
        except FileNotFoundError: 
            with open(self._tracking_file_path, "wb") as f:
                pickle.dump({"pickle_cant_be_empty":self._template_subdict},f)
                print("Created new file for RunTracker")
            return self._load_tracking_data(pipeline_names)
                 
    def _check_loaded_data(self,pipeline_names):
        keys = self.tracking_data.keys()
        subkeys = self._template_subdict.keys()
        made_a_change = False

        for pipeline_name in pipeline_names:
            #If pipeline not in first level of dict add it using template
            if pipeline_name not in keys:
                new_pipeline = {pipeline_name: self._template_subdict}
                self.tracking_data |= new_pipeline
                made_a_change = True
                #No reason to check subkeys if its a new added pipeline
                continue
            #Check if all subkeys are available for loaded pipelines
            for subkey in subkeys:
                if subkey not in self.tracking_data[pipeline_name].keys():
                    self.tracking_data[pipeline_name][subkey] = self._template_subdict[subkey]
                    made_a_change = True
        #If any changes has been made overwrite the old data
        if made_a_change:
            with open(self._tracking_file_path, "wb") as f:
                pickle.dump(self.tracking_data,f)

        return self.tracking_data


    def update(self,pipeline_name: str, field: str, new_value):
        self.tracking_data[pipeline_name][field] = new_value
        with open(self._tracking_file_path, "wb") as f:
            pickle.dump(self.tracking_data,f)