from re import sub, match
from .constants import AIE_ID_SEPARATOR
from urllib.parse import unquote_plus
from typing import Any, TypedDict
from usaddress import tag

class Address(TypedDict, total=False):
  street: str
  unit: str
  city: str
  state: str
  zipcode: str

street_keys = ['AddressNumber', 'AddressNumberSuffix', 'StreetNamePreDirectional', 'StreetName' , 'StreetNamePostType']
unit_keys = ['OccupancyType', 'OccupancyIdentifier']


def toSalesforcePhone(input_data: Any) -> str:
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


def toSalesforceEmail(input_data: Any) -> str:
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


def extractId(input_data: Any) -> list[str]:
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

def extract_address(input_data: Any) -> Address:
  result = tag(f'{input_data}'.replace('\n', ' ').replace('\t', ' '))
  # If not enough data, return empty address
  if result[1] == 'Ambiguous':
    return Address()
  extracted_address = Address()
  # extracting street
  street_words = []
  for key in street_keys:
    if key in result[0]:
      street_words.append(result[0][key])
  extracted_address['street'] = ' '.join(street_words)
  # extrating unit
  unit_words = []
  for key in unit_keys:
    if key in result[0]:
      unit_words.append(result[0][key])
  extracted_address['unit'] = ' '.join(unit_words)
  # extracting city
  if 'PlaceName' in result[0]:
    extracted_address['city'] = result[0]['PlaceName']
  # extrating zipcode
  if 'ZipCode' in result[0]:
    extracted_address['zipcode'] = result[0]['ZipCode']
  return extracted_address