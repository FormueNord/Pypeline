import os
import imaplib
import os.path    
import email
from datetime import timedelta, datetime
import msal

def function_constructor(func):
    """
    Decorator used to construct a function
    """
    def wrapper(folder_path, **kwargs):
        return lambda: func(folder_path, **kwargs)
    return wrapper

@function_constructor
def folder_monitor_constructor(folder_path, in_name_criteria = None):
    """
    Constructs a function that monitors folder content and returns values based on content and set criteria

    Args:
        folder:  specify path to monitored folder
        in_name_criteria:  filter out any files in folder not containing the criteria in name

    Returns:
        List of file names in folder containing criteria (all files if no criteria is set) 
    """
    files_in_folder = os.listdir(folder_path)
    if in_name_criteria:
        trigger_files = [file for file in files_in_folder if in_name_criteria in file]
        trigger_files = [os.path.join(folder_path,file) for file in trigger_files]
        return trigger_files
    return [os.path.join(folder_path,file) for file in files_in_folder]

class emailFetcher:
    """
    Creates instance of emailFetcher which logs onto the relevant office365 email.

    Args:
    username:str = email username
    password:str = email password hashed using b64encode
    email_folder:str = the folder within the email to be searched. Defaults to inbox
    

    Returns:
    instance of emailFetcher
    """

    def __init__(self,username:str,password:str,email_folder : str = 'Inbox'):
        self.USERNAME = username
        self.PASSWORD = password

        self.imap =  imaplib.IMAP4_SSL('outlook.office365.com')
        config = {"authority": "https://login.microsoftonline.com/4b2541b6-173d-4cfb-956e-bc1b3d46a48e",
            "scope" : ["https://outlook.office.com/IMAP.AccessAsUser.All"],
            "secret": "UqK8Q~ezRxPZHZG96Wh.pBZXKDrMHUFAfSsRLaNZ",
            "client_id":"86503419-481a-4d07-ba16-bcfad020b753",
            "endpoint": "https://outlook.office.com/IMAP.AccessAsUser.All"}

        app = msal.ConfidentialClientApplication(
        config["client_id"], authority=config["authority"],
        client_credential=config["secret"],
        )

        app = msal.PublicClientApplication(config["client_id"], authority=config["authority"])
        flow = app.initiate_device_flow(scopes=config["scope"])
        result = app.acquire_token_by_device_flow(flow)["access_token"]
        access_string = self.generate_oauth2_string("ca@formuenord.dk", result, False)
        self._checkForFailure(self.imap.authenticate('XOAUTH2', lambda x: access_string))
        self._checkForFailure(self.imap.select(email_folder))

    def __exit___(self):
        #close connection to email when script ends
        self.imap.close()

    def generate_oauth2_string(self, username, access_token, as_base64=False):
        import base64
        auth_string = 'user=%s\1auth=Bearer %s\1\1' % (username, access_token)
        auth_string = auth_string.encode("utf-8")
        if as_base64:
            auth_string = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
        return auth_string
    
    def create_new_oauth_flow(self):
        config = {"authority": "https://login.microsoftonline.com/4b2541b6-173d-4cfb-956e-bc1b3d46a48e",
            "scope" : ["https://outlook.office.com/IMAP.AccessAsUser.All"],
            "secret": "UqK8Q~ezRxPZHZG96Wh.pBZXKDrMHUFAfSsRLaNZ",
            "client_id":"86503419-481a-4d07-ba16-bcfad020b753",
            "endpoint": "https://outlook.office.com/IMAP.AccessAsUser.All"}


        # Create a preferably long-lived app instance which maintains a token cache.
        app = msal.ConfidentialClientApplication(
            config["client_id"], authority=config["authority"],
            client_credential=config["secret"],
            )

        app = msal.PublicClientApplication(config["client_id"], authority=config["authority"])
        flow = app.initiate_device_flow(scopes=config["scope"])
        print(flow)
        return flow, app
    
    def create_new_oauth_token(self,flow,app):
        result = app.acquire_token_by_device_flow(flow)
        return result

    def search_for_emails(self,expected_from_adress: str, expected_subject: str, look_day_back: int = 1) -> email.message.Message:
        """
        Searches for mails with the specified from adress and subject line.

        Args:
            expected_from_adress:str = the from adress for which the email is expected.
            expected_subject:str = part of the subject line for the email which is searched for.
            look_day_back:int = the amount of days back the scripts looks to search for the email.

        Returns:
            A list of Message instances
        """

        #search for mails after prior_date
        prior_date = datetime.now() - timedelta(look_day_back)
        timeObj = prior_date.strftime('%d-%b-%Y')
        timeObjString = f'SINCE "{timeObj}"'
        numbers = self._checkForFailure(self.imap.uid('search', None,timeObjString))

        #if no mails simply return to avoid unnecessary error notification
        #temporary check to see whether this part of the code is responsible for an annoying error

        if len(numbers[0]) == 0:
            return

        #get uids and the instances of Message
        uids = numbers[0].decode('utf-8').split()
        messages = self._checkForFailure(self.imap.uid('fetch', ','.join(uids), '(RFC822)'))

        #look for any mails matching criterias
        relevant_messages = []
        for idx,(_, message) in enumerate(messages[::2]):
            msg = email.message_from_string(message.decode('utf-8'))
            subject = msg.get('subject')
            subject = self._clean_str_format(subject)
            from_adress = msg.get('from')
            from_adress = from_adress[from_adress.find('<') + 1:].replace('>','')

            if expected_subject in subject and expected_from_adress in from_adress:
                relevant_messages.append([msg,uids[idx]])
            
        return relevant_messages 
    
    def get_attachments(self,messages:list[email.message.Message], folder_path: str, email_folder_path: str) -> list[str]:
        """
        Downloads and saves attachments from emails and moves the email to the specified email_folder_path. 
        Takes the result of search_for_email as arg.

        Args:
            messages: list[email.message.message,uid] = list of instances of Message. Is the output of search_for_email
            folder_path: str = an absolute path to the folder which the attachments are saved to
            email_folder_path: str = a path to the email folder in which emails are placed after their attachments are downloaded

        Returns:
            Name of the local file name if any files are saved from email attachments.
        """

        #temporary check to see whether this part of the code is responsible for an annoying error
        try:
            if len(messages) == 0:
                return
        except Exception:
            raise "Fejl i den anden len i emailFetcher"
        
        #get attachments from mails in messages
        file_paths = []
        for message in messages:
            uid = message[1]
            for part in message[0].walk():
                if part.get_content_maintype() != 'multipart' and part.get('Content-Disposition') is not None:
                    file_data = part.get_payload(decode = True)
                    file_name = self._clean_str_format(part.get_filename())
                    file_path = os.path.join(folder_path,file_name)
                    with open(file_path,'wb') as f:
                        f.write(file_data)
                    file_paths.append(file_path)

                    #https://stackoverflow.com/questions/3527933/move-an-email-in-gmail-with-python-and-imaplib
                    #copy mail to the email_folder_path
                    self._checkForFailure(self.imap.uid("COPY", uid, email_folder_path))
                    self._checkForFailure(self.imap.uid('STORE', uid , '+FLAGS', '(\Deleted)'))
                    self.imap.expunge() 
        return file_paths

    def search_and_get_attachments(self, expected_from_adress: str, expected_subject: str, folder_path: str, email_folder_path: str):
        """
        Runs search_for_email and get_attachments
        """
        lst_msg_uid = self.search_for_emails(expected_from_adress, expected_subject)
        if not lst_msg_uid:
            return
        abs_file_paths = self.get_attachments(lst_msg_uid, folder_path, email_folder_path)
        return abs_file_paths

    def _checkForFailure(self,returned):
        result, other = returned
        if result != 'OK':
            raise Exception('IMAP failed')
        return other

    def _clean_str_format(self,string:str):
        return string.replace('\r','').replace('\n','')

