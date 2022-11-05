from re import sub, match
from .constants import AIE_ID_SEPARATOR
from urllib.parse import unquote_plus


def toSalesforcePhone(input_data):
    '''
      Convert anything to phone number Will return empty string if didn't find anything.
    '''

    # Serial to string to avoid error data type
    text_input = f'{input_data}'

    # Phone number should have minimum 10 return nothing if less
    if len(text_input) < 10:
        return ''

    # Extract number from a string
    cleaned_phone = sub(r'[^0-9]', '', text_input)

    # Remove 1 area code
    return cleaned_phone[0:10] if cleaned_phone[0] != '1' else cleaned_phone[1:11]


def toSalesforceEmail(input_data):
    '''
      Clean and extract email from input_data
    '''
    # Serial to string to avoid error data type
    text_input = f'{input_data}'

       # Return nothing if there's no @ and .
    if not '@' in text_input and not '.' in text_input:
      return ''

    # Split input_data and filter through it
    # https://www.emailregex.com/
    # Email validation
    possible_email = [email for email in text_input.split(' ') if match(r'^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\
.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$', email)]
    return possible_email[0].lower() if len(possible_email) > 0 else ''


def extractId(input_data):
    '''
      Extract AIE IDs from a string
    '''
    
    # decode from URL string
    decoded = unquote_plus(f'{input_data}')

    if len(decoded) < 21:
      return []

    # Keep only valid ID
    # ID only containes alphanumeric characters, if a word contains other than
    # alphanumeric, throw it out.
    possible_ids = [possible_id for possible_id in decoded.split(AIE_ID_SEPARATOR) if not '^' in sub(r'[&/\\#,+()$~%.\'":*?<>{} ]', '^', possible_id) and (len(possible_id) == 15 or len(possible_id) == 18)]

    return possible_ids
