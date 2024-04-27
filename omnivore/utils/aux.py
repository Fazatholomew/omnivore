from re import sub, match
from .constants import (
    AIE_ID_SEPARATOR,
    OPPORTUNITY_COLUMNS,
    CAMBRIDGE_OPPORTUNITY_COLUMNS,
    ACCOUNT_COLUMNS,
    CFP_TOWS,
    DATETIME_SALESFORCE,
)
from urllib.parse import unquote_plus
from typing import TypedDict, Dict, Any, Callable
from usaddress import tag, RepeatedLabelError
from pandas import DataFrame, isna, Series, to_datetime
from pandas.api.types import is_datetime64_any_dtype
from .types import Record_Find_Info, Account, Opportunity
from datetime import datetime
from collections import OrderedDict
import logging

# Create a logger object
logger = logging.getLogger(__name__)


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
    if "@" not in text_input and "." not in text_input:
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
        if "^" not in sub(r'[&/\\#,+()$~%.\'":*?<>{} ]', "^", possible_id)
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
    for city in CFP_TOWS.keys():
        if city in extracted_address["unit"].lower():
            extracted_address["unit"] = (
                extracted_address["unit"].lower().replace(f" {city}", "")
            )
            extracted_address["city"] = city
            break
    return extracted_address


def to_account_and_opportunities(input: DataFrame) -> list[Record_Find_Info]:
    """
    Group row with each other that has the same contact information or account
    """
    data: dict[str, Record_Find_Info] = {}
    mappings: dict[str, str] = {}
    results: list[Record_Find_Info] = []
    contact_fields: list[tuple[str, Callable[[Any], str]]] = [
        ("Phone", toSalesforcePhone),
        ("PersonEmail", toSalesforceEmail),
    ]
    try:
        if (
            contact_fields[0][0] not in input.columns
            and contact_fields[1][0] not in input.columns
        ):
            # don't process if there are no contact columns
            return []
        for row in input.itertuples():
            try:
                available_columns = []
                for field, func in contact_fields:
                    if hasattr(row, field):
                        current_contact_info = func(getattr(row, field))
                        if len(current_contact_info) > 6:
                            available_columns.append((field, current_contact_info))
                            # (field_name, value)
                length_of_available_columns = len(available_columns)
                current_opp = Opportunity()
                current_account = Account()
                current_opps = []

                if length_of_available_columns > 0:
                    # Contact info is available
                    if length_of_available_columns == 1:
                        contact_key = available_columns[0][1]
                        if contact_key not in mappings:
                            mappings[contact_key] = contact_key

                    if length_of_available_columns == 2:
                        if (
                            available_columns[0][1] in mappings
                            and available_columns[1][1] in mappings
                        ):
                            # Both are set
                            contact_key = mappings[available_columns[0][1]]

                        if (
                            available_columns[0][1] in mappings
                            or available_columns[1][1] in mappings
                        ):
                            # Only one of contact is in the mappings
                            # Determine which is not in mappings
                            contact_key = (
                                available_columns[0][1]
                                if available_columns[0][1] in mappings
                                else available_columns[1][1]
                            )
                            # Set the new key to the existing
                            mappings[
                                (
                                    available_columns[0][1]
                                    if available_columns[0][1] not in mappings
                                    else available_columns[1][1]
                                )
                            ] = contact_key

                        if (
                            available_columns[0][1] not in mappings
                            and available_columns[1][1] not in mappings
                        ):
                            # None of the contacts are in the mappings
                            contact_key = available_columns[0][1]
                            # Set both of the keys in mappings
                            mappings[contact_key] = contact_key
                            mappings[available_columns[1][1]] = contact_key

                    if mappings[contact_key] in data:
                        current_account = data[mappings[contact_key]]["acc"]
                        current_opps = data[mappings[contact_key]]["opps"]

                    else:
                        data[mappings[contact_key]] = Record_Find_Info(
                            acc=current_account, opps=current_opps
                        )

                    for current_contact_info in available_columns:
                        if current_contact_info[0] not in current_account:
                            current_account[current_contact_info[0]] = (
                                current_contact_info[1]
                            )

                else:
                    # No Contact info
                    results.append(
                        Record_Find_Info(acc=current_account, opps=[current_opp])
                    )

                # Clean address
                if hasattr(row, "Street__c"):
                    extracted_address = extract_address(getattr(row, "Street__c"))
                    # Set Account address if not set yet
                    for key, value in address_keys.items():
                        if key in extracted_address:
                            current_opp[value[0]] = extracted_address[key]
                            if key != "unit":
                                current_account[value[1]] = extracted_address[key]

                # Name
                full_name = []
                # First Name Last Name for Account
                if hasattr(row, "FirstName") and "FirstName" not in current_account:
                    first_name = getattr(row, "FirstName")
                    current_account["FirstName"] = (
                        first_name
                        if not isna(first_name) and first_name != ""
                        else "Unknown"
                    )
                    full_name.append(first_name)

                if hasattr(row, "LastName") and "LastName" not in current_account:
                    last_name = getattr(row, "LastName")
                    current_account["LastName"] = (
                        last_name
                        if not isna(last_name) and last_name != ""
                        else "Unknown"
                    )
                    full_name.append(last_name)
                full_name = [
                    current_item
                    for current_item in full_name
                    if not isna(current_item) and len(current_item) > 0
                ]
                if len(full_name) == 0:
                    full_name.append("Unknown")
                # Name for Opp
                current_opp["Name"] = " ".join(full_name)

                # Rest of data
                for column in OPPORTUNITY_COLUMNS + CAMBRIDGE_OPPORTUNITY_COLUMNS:
                    if hasattr(row, column) and column not in current_opp:
                        current_opp[column] = getattr(row, column)

                # Add tempId
                if hasattr(row, "tempId"):
                    current_opp["tempId"] = getattr(row, "tempId")

                for column in ACCOUNT_COLUMNS:
                    if hasattr(row, column) and column not in current_account:
                        current_account[column] = getattr(row, column)

                current_opps.append(current_opp)
            except Exception as e:
                logger.error(
                    f"Can't process:\n {getattr(row, 'ID_from_HPC__c') if hasattr(row, 'ID_from_HPC__c') else row}"
                )
                logger.error("An error occurred: %s", str(e), exc_info=True)

        for current_record_find_info in data.values():
            try:
                if len(current_record_find_info["opps"]) > 0:
                    for i in range(1, len(current_record_find_info["opps"])):
                        current_record_find_info["opps"][i][
                            "Name"
                        ] = f'{current_record_find_info["opps"][i]["Name"]} unit {i}'
                results.append(current_record_find_info)
            except Exception as e:
                logger.error(
                    f"Error trying to update unit for landlords:\n {current_record_find_info['acc']}"
                )
                logger.error("An error occurred: %s", str(e), exc_info=True)

        return results
    except Exception as e:
        logger.error("Unhandled Error")
        logger.error("An error occurred: %s", str(e), exc_info=True)
        return results


def toSalesforceMultiPicklist(
    input: Any, separator: str = ";", mapper: dict = None
) -> str:
    converted = f"{input}"
    results = []
    splitted = converted.split(separator)
    for current_item in splitted:
        if mapper:
            if current_item in mapper:
                results.append(mapper[current_item])
        else:
            results.append(current_item)
    return ";".join(results)


def to_sf_payload(
    data: Dict[str, Any] | Opportunity | Account, object_type: str = "Account"
) -> Dict[str, Any]:
    """
    Transform payload to only include SF Field.
    """
    columns = (
        OPPORTUNITY_COLUMNS + CAMBRIDGE_OPPORTUNITY_COLUMNS
        if object_type == "Opportunity"
        else ACCOUNT_COLUMNS
    )
    return {
        key: sanitize_data_for_sf(data[key])
        for key in columns
        if key in data and (isinstance(data[key], list) or not isna(data[key]))
    }


def find_cfp_campaign(data: Opportunity) -> str:
    """
    Return respected CFP Campaign
    """
    if "City__c" not in data:
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
    for current_column in [source_column, first_name_column, last_name_column]:
        mask = copied[current_column].isna() | (copied[current_column] == "")
        copied.loc[mask, current_column] = data[source_column]
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
    return copied.dt.strftime(DATETIME_SALESFORCE)


def save_output_df(data: DataFrame, name: str = "General", file_type: str = "csv"):
    if file_type == "csv":
        data.to_csv(f"Debug {name}.csv", index=False)
    if file_type == "json":
        data.to_json(
            f"Debug {name}.json", index=False, orient="records", date_format="iso"
        )


def instance_to_dict(input: Any) -> dict:
    result = {}
    for key, value in input.__dict__.items():
        if isinstance(value, datetime):
            result[key] = value.timestamp()
        else:
            result[key] = value

    return result


def combine_xy_columns(
    _data: DataFrame, columns: list[str], primary="x", secondary="y"
) -> DataFrame:
    data = _data.copy()
    if len(columns) == 0:
        for column in data.columns:
            if f"_{primary}" in column:
                column_name = column[:-2]
                if f"{column_name}_{secondary}" in data.columns:
                    data[column_name] = data[f"{column_name}_{primary}"].combine_first(
                        data[f"{column_name}_{secondary}"]
                    )
        return data

    for column in columns:
        primary_column = f"{column}_{primary}"
        secondary_column = f"{column}_{secondary}"
        if primary_column not in data.columns and secondary_column not in data.columns:
            continue
        data[column] = data[primary_column].combine_first(data[secondary_column])
    return data
