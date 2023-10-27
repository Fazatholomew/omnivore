from re import sub, match
from .constants import (
    AIE_ID_SEPARATOR,
    OPPORTUNITY_COLUMNS,
    ACCOUNT_COLUMNS,
    CFP_TOWS,
    DATETIME_SALESFORCE,
)
from urllib.parse import unquote_plus
from typing import TypedDict, Dict, Any
from usaddress import tag, RepeatedLabelError
from pandas import DataFrame, isna, Series, to_datetime
from pandas.api.types import is_datetime64_any_dtype
from .types import Record_Find_Info, Account, Opportunity
from datetime import datetime
from collections import OrderedDict


class Address(TypedDict, total=False):
    street: str
    unit: str
    city: str
    state: str
    zipcode: str


street_keys = [
    "AddressNumber",
    "AddressNumberSuffix",
    "StreetNamePreDirectional",
    "StreetName",
    "StreetNamePostType",
]
unit_keys = ["OccupancyType", "OccupancyIdentifier"]
address_keys = {
    "street": ["Street__c", "BillingStreet"],
    "city": ["City__c", "BillingCity"],
    "zipcode": ["Zipcode__c", "BillingPostalCode"],
    "unit": ["Unit__c"],
}


def toSalesforcePhone(input_data: Any) -> str:
    """
    Convert anything to phone number Will return empty string if didn't find anything.
    """

    # Serial to string to avoid error data type
    text_input = f"{input_data}"

    # Phone number should have minimum 10 return nothing if less
    if len(text_input) < 10:
        return ""

    # Extract number from a string
    cleaned_phone = sub(r"[^0-9]", "", text_input)

    if len(cleaned_phone) == 0:
        return ""

    # Remove 1 area code
    return cleaned_phone[0:10] if cleaned_phone[0] != "1" else cleaned_phone[1:11]


def toSalesforceEmail(input_data: Any) -> str:
    """
    Clean and extract email from input_data
    """
    # Serial to string to avoid error data type
    text_input = f"{input_data}"

    # Return nothing if there's no @ and .
    if not "@" in text_input and not "." in text_input:
        return ""

    # Split input_data and filter through it
    # https://www.emailregex.com/
    # Email validation
    possible_email = [
        email
        for email in text_input.split(" ")
        if match(
            r'^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\
.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$',
            email,
        )
    ]
    return possible_email[0].lower() if len(possible_email) > 0 else ""


def extractId(input_data: Any) -> list[str]:
    """
    Extract AIE IDs from a string
    """

    # decode from URL string
    decoded = unquote_plus(f"{input_data}")

    if len(decoded) < 21:
        return []

    # Keep only valid ID
    # ID only containes alphanumeric characters, if a word contains other than
    # alphanumeric, throw it out.
    possible_ids = [
        possible_id
        for possible_id in decoded.split(AIE_ID_SEPARATOR)
        if not "^" in sub(r'[&/\\#,+()$~%.\'":*?<>{} ]', "^", possible_id)
        and (len(possible_id) == 15 or len(possible_id) == 18)
    ]

    return possible_ids


def extract_address(input_data: Any) -> Address:
    try:
        result = tag(f"{input_data}".replace("\n", " ").replace("\t", " "))
    except RepeatedLabelError as e:
        parsed = OrderedDict()
        for value, key in e.parsed_string:
            if key in parsed:
                continue
            parsed[key] = value
        result = [parsed, "Street Address"]
    # If not enough data, return empty address
    if result[1] == "Ambiguous":
        return Address()
    extracted_address = Address()
    # extracting street
    street_words = []
    for key in street_keys:
        if key in result[0]:
            street_words.append(result[0][key])
    extracted_address["street"] = " ".join(street_words)
    # extrating unit
    unit_words = []
    for key in unit_keys:
        if key in result[0]:
            unit_words.append(result[0][key])
    extracted_address["unit"] = " ".join(unit_words)
    # extracting city
    if "PlaceName" in result[0]:
        extracted_address["city"] = result[0]["PlaceName"]
    # extrating zipcode
    if "ZipCode" in result[0]:
        extracted_address["zipcode"] = result[0]["ZipCode"][:10]
    return extracted_address


def to_account_and_opportunities(data: DataFrame) -> list[Record_Find_Info]:
    """
    Group row with each other that has the same contact information or account
    """
    available_columns = [
        column for column in ["Phone", "PersonEmail"] if column in data.columns
    ]
    length_of_available_columns = len(available_columns)
    if length_of_available_columns == 0 or len(data) == 0:
        # No contact info, don't proccess it
        return []
    if length_of_available_columns == 1:
        sorted_data = data.sort_values(available_columns, na_position="first")
        result: list[Record_Find_Info] = []
        contact_column = available_columns[0]
        current_account = Account()
        current_account[contact_column] = sorted_data.iloc[0][contact_column]
        current_opps: list[Opportunity] = []
        for row in sorted_data.itertuples():
            current_opp = Opportunity()
            contact_info = getattr(row, contact_column)
            # check if both contact info are different
            if contact_info != current_account[contact_column]:
                # already different account, append the current account
                result.append(Record_Find_Info(acc=current_account, opps=current_opps))
                current_account = Account()
                current_account[contact_column] = contact_info
                current_opps: list[Opportunity] = []

            # Clean address
            if hasattr(row, "Street__c"):
                extracted_address = extract_address(getattr(row, "Street__c"))
                # Set Account address if not set yet
                if not "BillingStreet" in current_account:
                    for key, value in address_keys.items():
                        if key in extracted_address:
                            current_opp[value[0]] = extracted_address[key]
                            if key != "unit":
                                current_account[value[1]] = extracted_address[key]

            # Name
            full_name = []
            # First Name Last Name for Account
            if hasattr(row, "FirstName") and not "FirstName" in current_account:
                first_name = getattr(row, "FirstName")
                current_account["FirstName"] = first_name
                full_name.append(first_name)

            if hasattr(row, "LastName") and not "LastName" in current_account:
                last_name = getattr(row, "LastName")
                current_account["LastName"] = last_name
                full_name.append(last_name)

            # Name for Opp
            # current_opp['Name'] =  ' '.join(full_name) if len(current_opps) == 0 else f"{' '.join(full_name)} unit {len(current_opps)}"

            # Rest of data
            for column in OPPORTUNITY_COLUMNS:
                if hasattr(row, column) and not column in current_opp:
                    current_opp[column] = getattr(row, column)

            # Add tempId
            if hasattr(row, "tempId"):
                current_opp["tempId"] = getattr(row, "tempId")

            for column in ACCOUNT_COLUMNS:
                if hasattr(row, column) and not column in current_account:
                    current_account[column] = getattr(row, column)

            current_opps.append(current_opp)

        result.append(Record_Find_Info(acc=current_account, opps=current_opps))
        return result

    if len(available_columns) == 2:
        # If more than one contact information are used, use the sorting method
        data["PersonEmail"] = data["PersonEmail"].fillna(data["Phone"])
        data["Phone"] = data["Phone"].fillna(data["PersonEmail"])
        sorted_data = data.sort_values(available_columns, na_position="first")
        result: list[Record_Find_Info] = []
        current_account = Account(
            PersonEmail=toSalesforceEmail(sorted_data.iloc[0]["PersonEmail"]),
            Phone=toSalesforcePhone(sorted_data.iloc[0]["Phone"]),
        )
        current_opps: list[Opportunity] = []
        for row in sorted_data.itertuples():
            current_opp = Opportunity()
            email = toSalesforceEmail(getattr(row, "PersonEmail"))
            phone = toSalesforcePhone(getattr(row, "Phone"))
            # check if both contact info are different
            if (
                email != current_account["PersonEmail"]
                and phone != current_account["Phone"]
            ):
                # already different account, append the current account
                result.append(Record_Find_Info(acc=current_account, opps=current_opps))
                current_account = Account(PersonEmail=email, Phone=phone)
                current_opps = []

            # If account doesn't have email yet, fill it up
            if len(email) > 0 and len(current_account["PersonEmail"]) == 0:
                current_account["PersonEmail"] = email

            # If account doesn't have phone yet, fill it up
            if len(phone) > 0 and len(current_account["Phone"]) == 0:
                current_account["Phone"] = phone

            # Clean address
            if hasattr(row, "Street__c"):
                extracted_address = extract_address(getattr(row, "Street__c"))
                # Set Account address if not set yet
                if not "BillingStreet" in current_account:
                    for key, value in address_keys.items():
                        if key in extracted_address:
                            current_opp[value[0]] = extracted_address[key]
                            if key != "unit":
                                current_account[value[1]] = extracted_address[key]
                #   current_account['BillingStreet'] = extracted_address['street'] if not 'BillingStreet' in current_account else current_account['BillingStreet']
                #   current_account['BillingCity'] = extracted_address['city'] if not 'BillingCity' in current_account else current_account['BillingCity']
                #   current_account['BillingPostalCode'] = extracted_address['zipcode'] if not 'BillingPostalCode' in current_account else current_account['BillingPostalCode']

                # current_opp['Street__c'] = extracted_address['street']
                # current_opp['City__c'] = extracted_address['city']
                # current_opp['Zipcode__c'] = extracted_address['zipcode']
                # current_opp['Unit__c'] = extracted_address['unit']

            # Name
            full_name = []
            # First Name Last Name for Account
            if hasattr(row, "FirstName") and not "FirstName" in current_account:
                first_name = getattr(row, "FirstName")
                if not isna(first_name):
                    current_account["FirstName"] = first_name
                    full_name.append(first_name)

            if hasattr(row, "LastName") and not "LastName" in current_account:
                last_name = getattr(row, "LastName")
                if not isna(last_name):
                    current_account["LastName"] = last_name
                    full_name.append(last_name)

            # Name for Opp
            current_opp["Name"] = (
                " ".join(full_name)
                if len(current_opps) == 0
                else f"{' '.join(full_name)} unit {len(current_opps)}"
            )

            # Rest of data
            for column in OPPORTUNITY_COLUMNS:
                if hasattr(row, column) and not column in current_opp:
                    current_opp[column] = getattr(row, column)

            # Add tempId
            if hasattr(row, "tempId"):
                current_opp["tempId"] = getattr(row, "tempId")

            for column in ACCOUNT_COLUMNS:
                if hasattr(row, column) and not column in current_account:
                    current_account[column] = getattr(row, column)

            current_opps.append(current_opp)

        result.append(Record_Find_Info(acc=current_account, opps=current_opps))
        return result

    return []


def to_sf_payload(
    data: Dict[str, Any] | Opportunity | Account, object_type: str = "Account"
) -> Dict[str, Any]:
    """
    Transform payload to only include SF Field.
    """
    columns = OPPORTUNITY_COLUMNS if object_type == "Opportunity" else ACCOUNT_COLUMNS
    return {
        key: sanitize_data_for_sf(data[key])
        for key in columns
        if key in data and (isinstance(data[key], list) or not isna(data[key]))
    }


def find_cfp_campaign(data: Opportunity) -> str:
    """
    Return respected CFP Campaign
    """
    if not "City__c" in data:
        return ""
    if isna(data["City__c"]):
        return ""
    for key, value in CFP_TOWS.items():
        if key in data["City__c"].lower():
            return value
    return ""


def is_float(num: Any) -> bool:
    """
    Helper function to check whether a string is a float
    """
    if isinstance(num, int) or isinstance(num, float):
        return True
    if isinstance(num, str):
        try:
            float(num)
            return True
        except ValueError:
            return False

    return False


def sanitize_data_for_sf(data: Any):
    """
    Convert any type of data into salesforce format
    """
    if isinstance(data, list):
        return ";".join([f"{el}" for el in data])

    if isna(data) or data in ["NaN", "NaT", "nan"]:
        return ""

    if isinstance(data, datetime):
        return data.astimezone().strftime("%Y-%m-%dT%H:%M:%S.000%z")

    data = f"{data}"

    if data.isdigit():
        return int(data)

    if is_float(data):
        return float(data)

    if data.lower() == "true":
        return True

    if data.lower() == "false":
        return False

    return data

    # data_ids = {}
    # combined_data = {}
    # processed_ids = set()
    # for row in data.itertuples():
    #   email = ''
    #   phone = ''
    #   # check email
    #   if hasattr(row, 'PersonEmail'):
    #     if not isna(getattr(row, 'PersonEmail')):
    #       email = getattr(row, 'PersonEmail')
    #       if not ('email', email) in data_ids:
    #         data_ids[('email', email)] = {
    #           'rows': set(),
    #           'another_key': ''
    #         }
    #   # check phone
    #   if hasattr(row, 'Phone'):
    #     if not isna(getattr(row, 'Phone')):
    #       phone = getattr(row, 'Phone')
    #       if not ('phone', phone) in data_ids:
    #         data_ids[('phone', phone)] = {
    #           'rows': set(),
    #           'another_key': ''
    #         }
    #   # set email
    #   if len(email) > 0:
    #     data_ids[('email', email)]['rows'].add(row)
    #     data_ids[('email', email)]['another_key'] = ('phone', phone)

    #   # set phone
    #   if len(phone) > 0:
    #     data_ids[('phone', phone)]['rows'].add(row)
    #     data_ids[('phone', phone)]['another_key'] = ('email', email)

    #   for key, value in data_ids.items():
    #     # combine email and phone
    #     if key in processed_ids:
    #       continue
    #     if len(value['another_key']) == 0:
    #       combined_data[(key,)] = value['rows']
    #       processed_ids.add(key)
    #     else:
    #       combined_data[(key, value['another_key'])] = value['rows'].union(data_ids[value['another_key']]['rows'])
    #       processed_ids.add(key)
    #       processed_ids.add(value['another_key'])

    #   # Transform in account and opps
    #   for key, value in combined_data.items():
    #     account = Account()
    #     opps: list[Opportunity] = []
    #     for id in key:
    #       if id[0] == 'phone':
    #         account['Phone'] = id[1]

    #       if id[0] == 'email':
    #         account['PersonEmail'] = id[1]
    #     for opp in value:
    #       new_opp = Opportunity()
    #       # Extracting Name
    #       if not isna(opp.FirstName):
    #         account['FirstName'] = opp.FirstName

    #       if not isna(opp.LastName):
    #         account['LastName'] = opp.LastName

    #       # Extracting Address
    #       if not isna(opp.Street__c):
    #         extracted_address = extract_address(opp.Street__c)
    #         account['BillingStreet'] = extracted_address['street'] if not 'BillingStreet' in account else account['BillingStreet']
    #         account['BillingCity'] = extracted_address['city'] if not 'BillingCity' in account else account['BillingCity']
    #         account['BillingPostalCode'] = extracted_address['zipcode'] if not 'BillingPostalCode' in account else account['BillingPostalCode']


def extract_firstname_lastname(
    data: DataFrame,
    source_column: str,
    first_name_column="FirstName",
    last_name_column="LastName",
) -> DataFrame:
    """Extact a full name column into First name and Last Name

    Keyword arguments:
    data -- Dataframe with name source
    source_column -- Column name with name source
    first_name_column -- New column name for firstname
    last_name_column -- New column name for lastname
    Return: New DataFrame with firstname lastname added column
    """

    copied = data.copy()
    stringed_data = copied[source_column].astype(str)
    copied[first_name_column] = stringed_data.str.extract(r"(.*?(?=[\wäöüß]+$))")
    copied[last_name_column] = stringed_data.str.extract(r"( \w+)$")
    copied[first_name_column] = copied[first_name_column].str.replace(
        r"( )$", "", regex=True
    )
    copied[last_name_column] = copied[last_name_column].str.replace(" ", "")
    copied[last_name_column] = copied[last_name_column].fillna(stringed_data)
    return copied


def to_sf_datetime(data: Series, format: str | None = None) -> Series:
    """Convert a series into Salesforce formated datetime

    Keyword arguments:
    data -- Series with either str or datetime elements
    format -- strptime format
    """
    copied = (
        data
        if is_datetime64_any_dtype(data)
        else to_datetime(data, format=format, errors="coerce")
    )
    return copied.dt.strftime(DATETIME_SALESFORCE).astype(str)
