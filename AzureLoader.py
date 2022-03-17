import os
import pyodbc
import ast
import pandas as pd

class AzureLoader:

    def __init__(self, azure_server, database, authentication = "ActiveDirectoryPassword"):
        self.azure_server = azure_server
        self.database = database
        self.authentication = authentication
        self.cred_file_name = "\\".join(__file__.split("\\")[0:-1]) + "\\cred_details.txt"
        
        #load credentials
        NEW_CRED = self._get_cred()
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
            content.pop(azure_server)
        #remove only specific database credentials
        else:
            content[azure_server].pop(database)

        self._write_credentials_file(content)
        return

    def _get_cred(self) -> bool:
        #check if file exists and read file
        FILE_EXISTS = os.path.isfile(self.cred_file_name)
        if FILE_EXISTS:
            content = self._read_credentials_file()

            #check if server is contained in file
            SERVER_IN_CONTENT = self.azure_server in content.keys()
            if SERVER_IN_CONTENT:
                content_server = content[self.azure_server]

                #check if database is within sliced data
                if self.database in content_server.keys():
                    content_database = content_server[self.database]
                    self._UID = content_database["UID"]
                    self._PWD = content_database["PWD"]
                    return False
                else:
                    print("Can't find the database login at server level")
            else:
                print("Can't find an associated server login")
        else:
            print("Can't find the file containing credentials file")
            SERVER_IN_CONTENT = False

        #prompt user if wanting to create new credentials
        return self.new_credential(FILE_EXISTS,SERVER_IN_CONTENT)
    
    def _new_credential(self, FILE_EXISTS = False, SERVER_IN_CONTENT = False) -> bool:
        answer = input("Do you wanna save new credentials? (Y/N): ")
        if answer == "Y":
            self._UID = input("Enter email: ")
            self._PWD = input("Enter password: ")

            #read credentials file or create empty content dict
            content = self._read_credentials_file() if FILE_EXISTS else {}

            #add nested dict to content if server not found
            if not SERVER_IN_CONTENT:
                content = content | {self.azure_server:{}}
            
            #add credentials for nested dict in content
            cred_dict = {"UID":self._UID, "PWD": self._PWD}
            content[self.azure_server][self.database] = cred_dict

            #test if new credentials can be used to login
            self._login()

            #hash and write content to file
            self._write_credentials_files(content)
        return True

    def _login(self) -> None:
        self.cnxn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server}"+
            ";SERVER="+self.azure_server+
            ";DATABASE="+self.database+
            ";UID="+self._UID+
            ";PWD="+self._PWD+
            ";Authentication="+self.authentication
        )
        return
    
    def _read_credentials_file(self) -> dict:
        with open(self.cred_file_name,"r") as f:
            content = bytes.fromhex(f.read())
        content = content.decode("utf-8")
        content = ast.literal_eval(content)
        return content

    def _write_credentials_file(self,content:str) -> None:
        content = str(content).encode("UTF-8").hex()
        with open(self.cred_file_name,"w") as f:
            f.write(content)


if __name__ == "__main__":
    df = pd.DataFrame([[1,1.1],[2,2.2],[3,3.3]],columns = ['ID','val'])

    loader = AzureLoader("formuenord.database.windows.net","formuenordDB")
    loader.execute(df,"INSERT","testTable")