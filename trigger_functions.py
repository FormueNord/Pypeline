import os
import imaplib
import os.path    
import email
from datetime import timedelta, datetime
import msal
import Othenticator

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


class Mail:

    files_in_dir_path = []
    dir_path = ""

    def __init__(self, uid: bytes,response: bytes):
        self.uid = uid
        self.message = self._construct_Message_obj(response)
    
    @staticmethod
    def _construct_Message_obj(response):
        return email.message_from_string(response[0][1].decode("utf-8"))
    
    def get_subject(self) -> str:
        subject = self.message.get("subject")
        return self._clean_str_format(subject)

    def get_from_adress(self) -> str:
        from_adress = self.message.get("from")
        return from_adress[from_adress.find('<') + 1:].replace('>','')

    def get_attachments(self,folder_path) -> str:
        file_paths = []
        
        self._file_names_in_directory(folder_path)
        for part in self.message.walk():
            if part.get_content_maintype() != 'multipart' and part.get('Content-Disposition') is not None:
                file_data = part.get_payload(decode = True)
                file_name = self._clean_str_format(part.get_filename())
                file_name = self._file_name_versioning(file_name)
                file_path = os.path.join(folder_path,file_name)
                with open(file_path,'wb') as f:
                    f.write(file_data)
                file_paths.append(file_path)
                self.files_in_dir_path.append(file_name)
        return file_paths

    def _file_names_in_directory(self,local_path):
        if self.dir_path == local_path:
            return self.files_in_dir_path
        elif self.dir_path == "":
            self.dir_path = local_path
        self.files_in_dir_path = os.listdir(local_path)
    
    def _file_name_versioning(self,file_name):
        version = 1
        original_file_name = file_name
        while file_name in self.files_in_dir_path:
            file_name_components = original_file_name.split(".")
            file_name = file_name_components[0] + "_" + str(version) + "." + file_name_components[1]
            version += 1
        return file_name

    @staticmethod
    def _clean_str_format(string:str):
        return string.replace('\r','').replace('\n','')


class Mailbox:
    """
    Creates instance of Mailbox which logs onto the relevant office365 email.

    Args:
    username:str = email username
    password:str = email password hashed using b64encode
    email_folder:str = the folder within the email to be searched. Defaults to inbox
    

    Returns:
    instance of Mailbox
    """
    loaded_mails = []

    def __init__(self,email: str, config: dict, IMAP_host: str = "outlook.office365.com"):
        self.imap =  imaplib.IMAP4_SSL(IMAP_host)
        self._IMAP_authenticate(email, config)
        self.imap.select("inbox")

    def _IMAP_authenticate(self, email, config):
        try:
            othenticator = Othenticator.Othenticator(config)
            othenticator.imap_authentication(username = email, imap_instance = self.imap)
        except: 
            raise Exception(f"Othenticator failed to authenticate email {email}")

    def __exit___(self):
        #close connection to email when script ends
        self.imap.close()
    
    def _format_search_criterias(self, *args) -> tuple:
        args = list(args)
        for i,arg in enumerate(args):
            # all the inputs for search method needs to be strings
            if isinstance(arg, datetime):
                arg = arg.strftime("%d-%b-%Y")
            # search strings needs to be surrounded by apostrophes
            if i % 2 == 1:
                args[i] = '"' + arg + '"'
        return tuple(args)

    def search_emails(self, *args, subject_exact_match = False) -> tuple:
        args = self._format_search_criterias(*args)
        uids = self._checkForFailure(self.imap.uid("search",None,*args))
        uids = uids[0].decode("utf-8").split()

        if len(uids) == 0:
            print("No emails found matching time criteria")
            return []

        imap_response = self._fetch_mails(uids)
        mails = self._construct_Mails(imap_response,uids)
        if subject_exact_match:
            if not "SUBJECT" in args:
                raise Exception ("To do exact subject matching SUBJECT must be one of the search criterias")
            expected_subject = args[args.index("SUBJECT") + 1]
            self.loaded_mails = self._subject_exact_match(mails, expected_subject)
        return mails

    def get_attachments(self,file_path):
        file_paths = []
        try:
            for mail in self.loaded_mails:
                temp_file_paths = mail.get_attachments(file_path)
                file_paths.append(temp_file_paths)
            return file_paths
        except:
            raise Exception("Failed to fetch attachments from loaded messages")
    
    def move_messages(self, email_folder_path):
        for mail in self.loaded_mails:
            self._checkForFailure(self.imap.uid("COPY", mail.uid, email_folder_path))
            self._checkForFailure(self.imap.uid("STORE", mail.uid, "+FLAGS", "(\Deleted)"))
            self.imap.expunge()

    def _construct_Mails(self, response:list[tuple,bytes],uids:list[bytes]) -> list[Mail]:
        mails = []
        for i in range(len(uids)):
            mails.append(Mail(uid = uids[i], response = response[i*2 : (i+1)*2]))
        return mails

    def _subject_exact_match(self,mails: list[Mail],expected_subject):
        relevant_messages = []
        for mail in mails:
            if expected_subject == '"' + mail.get_subject() + '"':
                relevant_messages.append(mail)
        return relevant_messages

    def _fetch_mails(self,uids):
        messages = self._checkForFailure(self.imap.uid('fetch', ','.join(uids), '(RFC822)'))
        return messages

    def _checkForFailure(self,returned):
        result, other = returned
        if result != 'OK':
            raise Exception('IMAP failed')
        return other



