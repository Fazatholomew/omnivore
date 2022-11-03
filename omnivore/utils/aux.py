from re import sub


def toSalesforcePhone(input):
    '''
      Convert anything to phone number Will return empty string if didn't find anything.
    '''

    # Serial to string to avoid error data type
    text_input = f'{input}'

    # Phone number should have minimum 10 return nothing if less
    if len(text_input) < 10:
        return ''

    # Extract number from a string
    cleaned_phone = sub(r'[^0-9]', '', text_input)

    # Remove 1 area code
    return cleaned_phone[0:10] if cleaned_phone[0] != '1' else cleaned_phone[1:11]


def toSalesforceEmail(input):
    # Serial to string to avoid error data type
    text_input = f'{input}'

    # Return nothing if there's no @ and .
    if not '@' in text_input and not '.' in text_input:
        return ''

    # Split input and filter through it
    # https://www.emailregex.com/
    # Email validation
    possible_email = [email for email in text_input.split(' ') if match(r'^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\
.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$', email)]
    return possible_email[0] if len(possible_email) > 0 else ''
