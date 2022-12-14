import pytest
from Pypeline import AzureLoader
import ast
import pyodbc
import os
import pandas as pd
from datetime import date
import random


def read_load_destination_file():
    """
    Loads file used in tests.
    File should not be uploaded to github.

    If you want to run the tests yourself create a .txt file in the tests folder and write a valid AzureLoader.load_destination in there
    The AzureLoader.load_destination must refer to a table as specified (run SQL to create table):
        SET ANSI_NULLS ON
        GO
        SET QUOTED_IDENTIFIER ON
        GO
        CREATE TABLE [dbo].[testTable](
            [ID] [int] NULL,
            [val1] [real] NULL,
            [val2] [float] NULL,
            [date] [datetime2](7) NULL
        ) ON [PRIMARY]
        GO

        INSERT INTO dbo.testTable (ID, val1, val2, date)
        VALUES 
                (1,1,1,'2022-11-09') 
                (2,2,2,'2022-11-09')
    """

    with open(os.path.join("\\".join(__file__.split("\\")[0:-1]),"load_destination.txt"),"r") as f:
        content = f.read()
    content = ast.literal_eval(content)
    return content


def test_login():
    try:   
        loader = AzureLoader(read_load_destination_file())
    except pyodbc.Error as e:
        assert False, f"login failed for server sepecified in load_destination using stored credentials : {e}"
    return loader

# connection instance to be used throughout the tests
loader = test_login()


def test_insert():
    date_value = date.today()
    id_value = random.randint(1,10000000)
    test_data = {"ID": id_value, "val1": 1, "val2": [99.05,100], "date": date_value}
    test_data = pd.DataFrame.from_dict(test_data)

    try:
        loader.insert(test_data)
    except:
        assert False, f"""failed to insert data to server specificed in load_destination.
                        Does the test data match the test table, and does the test table exist?"""

    result = loader.get_all()
    result["date"] = result["date"].apply(lambda x: x.date())
    fetched_data = result.loc[(result["ID"] == id_value) & (result["date"] == date_value),:]
    assert len(fetched_data) != 0, """Data was not loaded to the database OR get_all fails"""


def test_get():
    columns = ["ID","val1","val2"]
    filter_string = "ID IS NOT NULL"

    result = loader.get(columns = columns, filter_string = filter_string)
    
    assert len(result.index) != 0, "The get function failed and returned empty handed. Some value with ID = 2 was expected"
    assert isinstance(result,pd.DataFrame), "AzureLoader.get() did not return a Pandas DataFrame"


def test_get_execute():
    result = loader.get_execute("SELECT * FROM dbo.testTable WHERE ID IS NOT NULL")

    assert len(result.index) != 0, "The get_execute function failed and returned empty handed. Atleast 1 row was expected"
    assert isinstance(result,pd.DataFrame), "the get_execute function did not return af Pandas DataFrame"


