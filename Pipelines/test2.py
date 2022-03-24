import sys
parent_dir = "\\".join(__file__.split("\\")[:-2])
sys.path.append(parent_dir)
import Pipeline

class nothing:
    pass

test2 = Pipeline.Pipeline(lambda: False, lambda: "test2", "", LoaderObj = nothing, error_notify_mails="ca@formuenord.dk")
