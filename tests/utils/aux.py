from re import sub

def toSalesforcePhone(input):
  # Serial to string to avoid error data type
  text_input = f'{input}'

  # Phone number should have minimum 10 return nothing if less
  if len(text_input) < 10:
    return ''

  # Extract number from a string 
  cleaned_phone = sub(r'[^0-9]', '', text_input)

  # Remove 1 area code
  return cleaned_phone[0:10] if cleaned_phone[0] != '1' else cleaned_phone[1:11]

