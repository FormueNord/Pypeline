import os
from email.mime.text import MIMEText
from smtplib import SMTP
from base64 import b64decode
import ast
import time

class ErrorAlerter():
    
    cred_file_name = "mail_cred_details.txt"

    def __init__(self,receivers: str, subject: str, warning_text: str):
        self.RECEIVERS = receivers
        self.WARNING_TEXT = warning_text
        self._load_credentials()
        self.SUBJECT = subject

    def _load_credentials(self):
        #create new file if none is available
        if self.cred_file_name not in os.listdir():
            print("No mail_cred_details.txt file found in root")
            self._create_new_credentials_file()
        with open(self.cred_file_name,"r") as f:
            content = f.read()
            content_str = bytes.fromhex(content).decode("UTF-8")
            content_dict = ast.literal_eval(content_str)
            self.UID, self.PWD = content_dict["UID"], content_dict["PWD"]
        print("read the ErrorAlert's credentials file!")
        return

    def _create_new_credentials_file(self):
        print("If you wanna create a new file, it has to be a microsoft mail, otherwise code needs changes")
        create_new = input("Do you wanna create a new credentials file? (Y/N):  ")
        if create_new:
            UID = input("Whats your mail adress?:  ")
            PWD = input("Whats your password?:   ")
            creds = {"UID":UID,"PWD":PWD}

            with open(self.cred_file_name,"w") as f:
                f.write(str(creds).encode("UTF-8").hex())
            
            print("New credentials file created!")
        else: 
            print("An error will be raised since no credentials are available")
        return

    def error_alert(self):
        self._setup_email()
        self._send_email()

    def _setup_email(self):
        self.RECEIVERS = self.RECEIVERS.split(',')
        message = MIMEText(str(self.WARNING_TEXT))
        message['Subject'] = self.SUBJECT
        message['From'] = self.UID
        message['To'] = self.RECEIVERS[0]
        message['Cc'] = ';'.join(self.RECEIVERS[1:])
        self.message = message
        return 
    
    #Currently not implemented since it would pause the Node for the duration of time.sleep
    #Fix at some point
    """def _reattempt_decorator(func):
    def wrapper(self):
        number_of_attempts = 0
        while number_of_attempts < 10:
            try: 
                func(self)
                return
            except:
                number_of_attempts += 1
            print('I failed')
            time.sleep(60*60)
            return
    return wrapper """

    def _send_email(self):
        with SMTP('smtp.office365.com',587) as server:
            server.ehlo()
            server.starttls()
            server.login(self.UID,self.PWD)
            server.sendmail(self.UID,self.RECEIVERS,self.message.as_string()) 
        return

if __name__ == "__main__":
    Alerter = ErrorAlerter(
        receivers = "ca@formuenord.dk",
        subject = "Tester bare ErrorAlerter",
        warning_text = "ikke noget vigtigt"
    )
    Alerter.error_alert()
    print("stop")