from Pypeline.ErrorAlerter import ErrorAlerter
import pytest


"""
If these tests fail, and its due to no credentials are being stored simply run the script and add some new credentials.
"""
def test_authentication():
    try:
        # email is old throwaway email. Spam it all you want
        alerter = ErrorAlerter("chris.albertsen90@gmail.com", "TEST ERROR ALERT FROM PYPELINE", "ERROR ALERT EXAMPLE TEST")
    except Exception as e:
        assert False, f"""Failed to initialize an ErrorAlerter obj with error message {e} .
                        The issue might be that no credentials are stored in the test folder for the ErrorAlerter"""
    return alerter

alerter = test_authentication()

def test_error_alert():
    result = alerter.error_alert()
    assert result == None, f"failed to send error alert using SMTP with error code(s){result}"


