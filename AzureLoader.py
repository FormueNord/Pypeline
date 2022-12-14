import os
import pyodbc
import ast
import pandas as pd
import numpy as np

class AzureLoader:
    """
    A class to manage loading from and to a specific Azure SQL Server database table using.
    Authentication is per default handled by ActiveDirectoryPassword, and the class will prompt for new credentials, the first time it is called.
    Different credentials can be stored for different Azure servers and or databases.
    A .txt file will be created containing the credentials in the folder containing the AzureLoader code. Be vary that the credentials are only encoded and hexified. CREDENTIALS ARE NOT ENCRYPTED.
    """

    _cred_file_name = "\\".join(__file__.split("\\")[0:-1]) + "\\cred_details.txt"

    def __init__(self, load_destination, authentication_type = "ActiveDirectoryPassword", testing_credentials = False):
        """
        Constructs an obj of AzureLoader

        INPUT:
            load_destination (dict[server, database, table]): specifies default load destination for the obj
        OPTIONAL:
        authentication_type (str): string specifying the ODBC authentication type. Default is ActiveDirectoryPassword
        testing_credentials (bool): used to test whether credentials are viable. If True validity of load_destination will not be asserted
        """
        
        self.load_destination = load_destination
        if not testing_credentials:
            self._assert_load_destination()
        self.authentication_type = authentication_type
        #load credentials
        NEW_CRED = self._get_credentials()
        #use loaded credentials to login and init instance of pyodbc.connect
        if not NEW_CRED:
            self._login()
        
    def insert(self,df: pd.DataFrame) -> None:
        """
        Insert Pandas DataFrame to table specified in constructor's "load_destination" parameter.
        Column names of DataFrame is matched with table names. 

        RETURNS:
            None
        """
        #transform np.nan vals to None
        df = df.replace([np.nan],[None])

        #create SQL code
        table = self.load_destination["table"]
        col_names = ",".join(df.columns)
        question_marks = ("?," * len(df.columns))[:-1]
        command_str = f"INSERT INTO {table} ({col_names}) VALUES ({question_marks})"

        #create instance of cursor
        cursor = self.cnxn.cursor()

        #execute many to iterate over list of values
        cursor.executemany(command_str, df.values.tolist())
        cursor.commit()
        return

    def get_all(self) -> pd.DataFrame:
        """
        Get all data for all columns in table

        RETURNS:
            Pandas DataFrame with all data from table
        """
        sql_string = f"SELECT * FROM {self.load_destination['table']}"
        return self.get_execute(sql_string)

    def get(self, columns: list[str], filter_string: str = "") -> pd.DataFrame:
        """
        Get data for specified columns.

        INPUT:
            columns (list[str]): list of column names to be fetched
        OPTIONAL: 
        filter_string (str): specify T-SQL filter ex. 'ID = 5 AND Date = '2022-11-09''
        
        RETURNS: 
            Pandas DataFrame with values of selected columns
        """
        select_string = "SELECT " + ",".join(columns)
        if filter_string != "":
            filter_string = "WHERE " + filter_string

        sql_string = f"{select_string} FROM {self.load_destination['table']} {filter_string}"

        return self.get_execute(sql_string)
    
    def get_execute(self,sql_string: str) -> pd.DataFrame:
        """
        Execute sql_string and return pandas dataframe.

        INPUT: 
            sql_string (str): sql string to be executed
        
        RETURNS:
            Query result as a Pandas DataFrame
        """
        sql_string = "".join(sql_string.splitlines())
        cursor = self.cnxn.cursor()
        result = cursor.execute(sql_string)
        cols = [col[0] for col in result.description]
        data = result.fetchall()
        return pd.DataFrame.from_records(data, columns = cols)

    def delete_credential(self, azure_server:str, database:str = None) -> None:
        """
        Deletes stores credentials for specified Azure SQL Server or if specified specific database on server.
        
        INPUT:
            azure_server (str): server for which credentials are to be deleted
        OPTIONAL:
        database (str): database on server for which credentials are to be deleted
        """
        content = self._read_credentials_file()
        
        #remove all server credentials
        if database == None:
            content.pop(self.load_destination["server"])
        #remove only specific database credentials
        else:
            content[self.load_destination["server"]].pop(database)

        self._write_credentials_file(content)
        return

    def _get_credentials(self) -> bool:
        """
        Loads credentials from credentials file.

        RETURNS:
            Bool indicating whether successful
        """
        #check if file exists and read file
        file_exists = os.path.isfile(self._cred_file_name)
        if not file_exists:
            print("Can't find the file containing credentials file")
            server_in_content = False
            return self.new_credential(file_exists,server_in_content)
        
        content = self._read_credentials_file()
        #check if server is contained in file
        server_in_content = self.load_destination["server"] in content.keys()
        if not server_in_content:
            print("Can't find an associated server login")
            return self.new_credential(file_exists,server_in_content)
        
        content_server = content[self.load_destination["server"]]
        #check if database is within sliced data
        if not self.load_destination["database"] in content_server.keys():
            print("Can't find the database login at server level")
            return self.new_credential(file_exists,server_in_content)
        content_database = content_server[self.load_destination["database"]]
        self._UID = content_database["UID"]
        self._PWD = content_database["PWD"]
        return False
    
    def new_credential(self, file_exists = False, server_in_content = False) -> bool:
        """
        Create new credentials for Azure SQL Server
        """
        answer = input("Do you wanna save new credentials for AzureLoader (Microsoft Active Directory credentials)? (Y/N): ")
        if answer == "N":
            return False
        self._UID = input("Enter email: ")
        self._PWD = input("Enter password: ")

        #read credentials file or create empty content dict
        content = self._read_credentials_file() if file_exists else {}

        #add nested dict to content if server not found
        if not server_in_content:
            content = content | {self.load_destination["server"]:{}}
        
        #add credentials for nested dict in content
        cred_dict = {"UID":self._UID, "PWD": self._PWD}
        content[self.load_destination["server"]][self.load_destination["database"]] = cred_dict

        #test if new credentials can be used to login
        self._login()

        #hash and write content to file
        self._write_credentials_file(content)
        return True

    def _login(self) -> None:
        try:
            self.cnxn = pyodbc.connect(
                "DRIVER={ODBC Driver 18 for SQL Server}"+
                ";SERVER="+self.load_destination["server"]+
                ";DATABASE="+self.load_destination["database"]+
                ";UID="+self._UID+
                ";PWD="+self._PWD+
                ";Authentication="+self.authentication_type
            )
        except(pyodbc.Error) as e: 
            print("""
                Authentication failed for specified authentication type.
                Attempts authentication using ActiveDirectoryInteractive for a MFA authentication-flow""")
            try:
                self.cnxn = pyodbc.connect(
                "DRIVER={ODBC Driver 18 for SQL Server}"+
                ";SERVER="+self.load_destination["server"]+
                ";DATABASE="+self.load_destination["database"]+
                ";UID="+self._UID+
                ";Authentication="+"ActiveDirectoryInteractive"
            )
            except(pyodbc.Error) as e:
                print("MFA authentication-flow failed as well")
                raise e
        return
    
    def _read_credentials_file(self) -> dict:
        with open(self._cred_file_name,"r") as f:
            content = bytes.fromhex(f.read())
        content = content.decode("utf-8")
        content = ast.literal_eval(content)
        return content

    def _write_credentials_file(self,content:str) -> None:
        content = str(content).encode("UTF-8").hex()
        with open(self._cred_file_name,"w") as f:
            f.write(content)

    def _assert_load_destination(self):
        """
        Asserts correctness of specified load_destination parameter given to constructor
        """
        fail_string = "load_destination must be a dict with a 'server','database' and 'table' keys"
        assert isinstance(self.load_destination,dict), fail_string
        keys = self.load_destination.keys()
        assert "server" in keys and "database" in keys and "table" in keys, fail_string
        return