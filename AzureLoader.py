import os
import pyodbc
import ast
import pandas as pd

class AzureLoader:

    _cred_file_name = "\\".join(__file__.split("\\")[0:-1]) + "\\cred_details.txt"


    def __init__(self, load_destination, authentication_type = "ActiveDirectoryPassword"):
        self.load_destination = load_destination
        self._assert_load_destination()
        self.authentication_type = authentication_type
        #load credentials
        NEW_CRED = self._get_credentials()
        #use loaded credentials to login and init instance of pyodbc.connect
        if not NEW_CRED:
            self._login()
        
    def insert(self,data, table:str) -> None:
        #create SQL code
        question_marks = ("?," * len(data.columns))[:-1]
        command_str = f"INSERT INTO {table} VALUES ({question_marks})"

        #create instance of cursor
        cursor = self.cnxn.cursor()

        #execute many to iterate over list of values
        #https://towardsdatascience.com/how-i-made-inserts-into-sql-server-100x-faster-with-pyodbc-5a0b5afdba5
        cursor.fast_executemany = True
        cursor.executemany(command_str, data.values.tolist())
        cursor.commit()
        return

    def delete_credential(self, azure_server:str, database = None) -> None:
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
        #check if file exists and read file
        file_exists = os.path.isfile(self._cred_file_name)
        if file_exists:
            content = self._read_credentials_file()

            #check if server is contained in file
            server_in_content = self.load_destination["server"] in content.keys()
            if server_in_content:
                content_server = content[self.load_destination["server"]]

                #check if database is within sliced data
                if self.load_destination["database"] in content_server.keys():
                    content_database = content_server[self.load_destination["database"]]
                    self._UID = content_database["UID"]
                    self._PWD = content_database["PWD"]
                    return False
                else:
                    print("Can't find the database login at server level")
            else:
                print("Can't find an associated server login")
        else:
            print("Can't find the file containing credentials file")
            server_in_content = False

        #prompt user if wanting to create new credentials
        return self.new_credential(file_exists,server_in_content)
    
    def new_credential(self, file_exists = False, server_in_content = False) -> bool:
        answer = input("Do you wanna save new credentials? (Y/N): ")
        if answer == "Y":
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
        self.cnxn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server}"+
            ";SERVER="+self.load_destination["server"]+
            ";DATABASE="+self.load_destination["database"]+
            ";UID="+self._UID+
            ";PWD="+self._PWD+
            ";Authentication="+self.authentication_type
        )
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
        fail_string = "load_destination must be a dict with a 'server','database' and 'table' keys"
        assert isinstance(self.load_destination,dict), fail_string
        keys = self.load_destination.keys()
        assert "server" in keys and "database" in keys and "table" in keys, fail_string
        return
        

if __name__ == "__main__":
    df = pd.DataFrame([[1,1.1],[2,2.2],[3,3.3]],columns = ['ID','val'])