from Pypeline.Node import Node
from Pypeline.Pipeline import Pipeline
import pytest
import os
import sys
from test_pipeline.pipeline_for_test import test_file_path, pipeline_for_test
from test_AzureLoader import read_load_destination_file
import copy

test_pipelines_folder = os.path.join("\\".join(__file__.split("\\")[0:-1]),"test_pipeline")

# replace the _credential_check method with this since it's not needed for the test
class NodeNoCredCheck(Node):

    def __init__(self, *args, **kwargs):
        super(NodeNoCredCheck, self).__init__(*args,**kwargs)

    def _credentials_check(self):
        return

def test_node_init():
    try:
        node = NodeNoCredCheck(test_pipelines_folder)
    except Exception as e:
        assert False, f"Node init failed with error: {e}"
    return node

node = test_node_init()

def test_node_monitor():
    try:
        node.monitor_pipelines(single_run = True)
    except Exception as e:
        assert False, f"Failed to run node.monitor_pipelines with error {e}"
    
    with open(test_file_path, "r") as f:
        file_text = f.read()
    
    assert file_text == "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]", "File created through monitor_pipelines did not contain the expected content"


def test_credentials_check():
    # credentials should not match anything in credentials file
    with pytest.raises(KeyError) as e:
        node = Node(test_pipelines_folder)

