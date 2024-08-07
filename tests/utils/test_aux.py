from omnivore.utils.aux import (
    toSalesforcePhone,
    toSalesforceEmail,
    extractId,
    extract_address,
    Address,
    to_account_and_opportunities,
    to_sf_payload,
    find_cfp_campaign,
    is_float,
    sanitize_data_for_sf,
    extract_firstname_lastname,
    to_sf_datetime,
    extract_zipcode,
)
from pandas import DataFrame, Series
from numpy import NaN
from omnivore.utils.constants import HEA_ID
from datetime import datetime
from pandas.testing import assert_frame_equal, assert_series_equal


def assert_partial_dict(dict_1, dict_2):
    for key in dict_2:
        if dict_1[key] != dict_2[key]:
            print(f"dict_1 {key} = {dict_1[key]} dict_2 {key} = {dict_2[key]}")
            return False
    return True


def test_toSalesforcePhone():
    # Test extracting number
    clean = toSalesforcePhone("askdfbashdfhsda123dsfasdf444asdfasdf---=='as4567")
    assert clean == "234444567"

    # Test to remove area code
    no_one = toSalesforcePhone("+1 917 333 3214")
    assert no_one == "9173333214"

    # Test only return 1 phone number that consists of 10 digits
    only_10 = toSalesforcePhone("+1 917 333 3214 +1 917 333 3214 +1 917 333 3214")
    assert len(only_10) == 10

    # Error prove if the input is not string
    nothing = toSalesforcePhone(None)
    assert nothing == ""

    # Error prove if the input is not string
    nothing = toSalesforcePhone({})
    assert nothing == ""

    # Error prove if the input is not string
    nothing = toSalesforcePhone([])
    assert nothing == ""

    # Convert to string
    phone_from_number = toSalesforcePhone(9123348123)
    assert phone_from_number == "9123348123"


def test_toSalesforceEmail():
    # return nothing if no email detected
    nothing = toSalesforceEmail([])
    assert nothing == ""

    # return nothing if no email detected
    nothing = toSalesforceEmail("sdfsdfdsf")
    assert nothing == ""

    # return nothing if no email detected
    nothing = toSalesforceEmail(12121344)
    assert nothing == ""

    # return nothing if no email detected
    nothing = toSalesforceEmail({})
    assert nothing == ""

    # Test multiple email, only get the first one
    first_email = toSalesforceEmail("garbafe@gmail.com   asdfasdfasdf@yahoo.com")
    assert first_email == "garbafe@gmail.com"

    # Lower all email
    lower_email = toSalesforceEmail("JffsImmY@yaHOO.com")
    assert lower_email == "jffsimmy@yahoo.com"

    # Don't know what happen
    assert "tinakemper1@verizon.net" == toSalesforceEmail("tinakemper1@verizon.net")


def test_extractId():
    # Return empty array when no id
    nothing = extractId(
        "asdfasdfasdfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsf"
    )
    assert nothing == []

    # ID shouldn't be less than 21 char
    no_id = extractId("<|>333ffffsf")
    assert no_id == []

    # Test extracting from encoded url
    decoded = extractId(
        "%3C%7C%3E00Q3i00000E6fVKEAZ%3C%7C%3E%20test%20test%20%3C%7C%3E00Q3i00000E6fVKEAZ%3C%7C%3E"
    )
    assert decoded == ["00Q3i00000E6fVKEAZ", "00Q3i00000E6fVKEAZ"]

    # From a paragraph
    parag = extractId(
        "test test \n \t \n <|>00Q3i00000E6fVKEAZ<|>sdfsdf this is just a test"
    )
    assert parag == ["00Q3i00000E6fVKEAZ"]


def test_extract_address():
    # Neeeco
    assert assert_partial_dict(
        extract_address("57 Codman Hill Avenue Boston, MA 02124    (617) 448-8742"),
        Address(street="57 Codman Hill Avenue", city="Boston", zipcode="02124"),
    )
    assert assert_partial_dict(
        extract_address("6 West Bedford Street Methuen, MA 01844      ("),
        Address(street="6 West Bedford Street", city="Methuen", zipcode="01844"),
    )
    assert assert_partial_dict(
        extract_address("212 Sycamore St, Unit 2, Watertown, MA 02472"),
        Address(
            street="212 Sycamore St", city="Watertown", unit="Unit 2", zipcode="02472"
        ),
    )
    assert assert_partial_dict(
        extract_address(
            """406 hollis st
01702"""
        ),
        Address(street="406 hollis st", zipcode="01702"),
    )
    assert assert_partial_dict(
        extract_address("22 Saxonia Ave Unit 1 Lawrence, MA 01841"),
        Address(
            street="22 Saxonia Ave", unit="Unit 1", city="Lawrence", zipcode="01841"
        ),
    )
    assert assert_partial_dict(
        extract_address("696 Bennington St, Unit 1, East Boston MA 02128"),
        Address(street="696 Bennington St", unit="Unit 1", zipcode="02128"),
    )
    # Error when address has phone number
    assert assert_partial_dict(
        extract_address(
            """1 Riverview Blvd 6-201 Methuen MA 01844

800-594-7277"""
        ),
        Address(
            street="1 Riverview Blvd", city="Methuen", zipcode="01844", unit="6-201"
        ),
    )

    # Homeworks
    assert assert_partial_dict(
        extract_address("(Unit 1) 117 Malden Street"),
        Address(street="117 Malden Street", unit="(Unit 1)"),
    )
    assert assert_partial_dict(
        extract_address("212 Webster St Malden unit 2"),
        Address(street="212 Webster St", unit="unit 2"),
    )
    assert assert_partial_dict(
        extract_address("(Unit B) 1084 Washington Street"),
        Address(street="1084 Washington Street", unit="(Unit B)"),
    )
    assert assert_partial_dict(
        extract_address("202 1/2 Bridge Street"),
        Address(street="202 1/2 Bridge Street"),
    )
    assert assert_partial_dict(
        extract_address("Unit(2) 36 Lothrop Street"),
        Address(street="36 Lothrop Street", unit="Unit (2)"),
    )
    assert assert_partial_dict(
        extract_address("153 Mount Vernon Street"),
        Address(street="153 Mount Vernon Street"),
    )
    assert assert_partial_dict(
        extract_address("(Unit 15 (1)) 15 Pierce Avenue"),
        Address(street="15 Pierce Avenue", unit="(Unit 15 (1))"),
    )
    # VHI
    assert assert_partial_dict(
        extract_address("99 Ferry St #3,"), Address(street="99 Ferry St", unit="# 3")
    )
    assert assert_partial_dict(
        extract_address("6 Nollet Drive AnDOVER"),
        Address(street="6 Nollet Drive", city="AnDOVER"),
    )
    assert assert_partial_dict(
        extract_address("3 Pearl Avenue, unit 2"),
        Address(street="3 Pearl Avenue", unit="unit 2"),
    )
    assert assert_partial_dict(
        extract_address("28 BUSWELL ST FL 2,"),
        Address(street="28 BUSWELL ST", unit="FL 2"),
    )


# Revise


def test_to_account_and_opp():
    def make_intermediate_dict(input, contact_field="PersonEmail"):
        # Use dict to map contact info, for reproducable tests
        results = {}
        for row in input:
            results[row["acc"][contact_field]] = row
        return results

    # One Contact info
    data = DataFrame(
        {"PersonEmail": [], "Street__c": [], "FirstName": [], "LastName": []}
    )
    assert to_account_and_opportunities(data) == []
    data = DataFrame(
        {
            "PersonEmail": ["test@gmail.com", "test1@gmail.com", "test@gmail.com"],
            "Street__c": ["test", "test", "test"],
            "FirstName": ["Jimmy", "not Jimmy", "yes Jimmy"],
            "LastName": ["last", "last", "last"],
            "ID_from_HPC__c": ["sdfasf", "asdffffff", "fffff"],
        }
    )
    result = to_account_and_opportunities(data)
    assert len(result) == 2
    temporary_dict = make_intermediate_dict(result)
    assert temporary_dict["test@gmail.com"]["acc"]["FirstName"] == "Jimmy"
    assert len(temporary_dict["test@gmail.com"]["opps"]) == 2
    assert temporary_dict["test1@gmail.com"]["opps"][0]["ID_from_HPC__c"] == "asdffffff"
    # Two contact info
    data = DataFrame(
        {
            "Phone": ["9163308234", "11111111", "9163308234"],
            "PersonEmail": ["test@gmail.com", "test1@gmail.com", NaN],
            "Street__c": ["test", "test", "test"],
            "FirstName": ["Jimmy", "not Jimmy", "yes Jimmy"],
            "LastName": ["last", "last", "last"],
            "ID_from_HPC__c": ["sdfasf", "asdffffff", "fffff"],
        }
    )
    result = to_account_and_opportunities(data)
    assert len(result) == 2
    temporary_dict = make_intermediate_dict(result, "Phone")
    assert temporary_dict["9163308234"]["acc"]["PersonEmail"] == "test@gmail.com"
    assert len(temporary_dict["9163308234"]["opps"]) == 2
    assert temporary_dict["11111111"]["opps"][0]["ID_from_HPC__c"] == "asdffffff"
    data = DataFrame(
        {
            "Phone": [NaN, "11111111", "9163308234"],
            "PersonEmail": ["test@gmail.com", "test1@gmail.com", "test@gmail.com"],
            "Street__c": ["test", "test", "test"],
            "FirstName": ["Jimmy", "not Jimmy", "yes Jimmy"],
            "LastName": ["last", "last", "last"],
            "ID_from_HPC__c": ["sdfasf", "asdffffff", "fffff"],
        }
    )
    result = to_account_and_opportunities(data)
    assert len(result) == 2
    temporary_dict = make_intermediate_dict(result)
    assert temporary_dict["test@gmail.com"]["acc"]["FirstName"] == "Jimmy"
    assert len(temporary_dict["test@gmail.com"]["opps"]) == 2
    assert temporary_dict["test1@gmail.com"]["opps"][0]["ID_from_HPC__c"] == "asdffffff"

    # No phone or email
    data = DataFrame(
        {
            "Phone": [NaN, "", ""],
            "PersonEmail": ["", NaN, NaN],
            "Street__c": ["test", "test", "test"],
            "FirstName": ["Jimmy", "not Jimmy", "yes Jimmy"],
            "LastName": ["last", "last", "last"],
            "ID_from_HPC__c": ["sdfasf", "asdffffff", "fffff"],
        }
    )
    result = to_account_and_opportunities(data)
    assert len(result) == 3


def test_to_sf_payload():
    # Account
    # Remove all data not a SF Field
    assert to_sf_payload(
        {
            "Name": "test",
            "asdfasdfasdf": "123123123",
            "FirstName": "HEHEH",
            "LastName": "Test",
        }
    ) == {"FirstName": "HEHEH", "LastName": "Test"}
    # Remove NaN
    assert to_sf_payload(
        {
            "Name": "test",
            "asdfasdfasdf": "123123123",
            "FirstName": NaN,
            "LastName": "Test",
        }
    ) == {"LastName": "Test"}
    # Doesn't touch number
    assert to_sf_payload({"Phone": 12345678}) == {"Phone": 12345678}

    # Opportunity
    assert to_sf_payload(
        {
            "Name": "test",
            "asdfasdfasdf": "123123123",
            "FirstName": "HEHEH",
            "LastName": "Test",
        },
        "Opportunity",
    ) == {"Name": "test"}

    # Empty array
    assert to_sf_payload(
        {"Name": [], "CloseDate": ["boom", "foo"]},
        "Opportunity",
    ) == {"Name": "", "CloseDate": "boom;foo"}


def test_find_cfp_campaign():
    # No city
    assert find_cfp_campaign({"City__c": ""}) == ""
    assert find_cfp_campaign({"City__c": NaN}) == ""  # type:ignore
    # find lower city
    assert find_cfp_campaign({"City__c": "beVErLy"}) == "7013i000000i5vOAAQ"


def test_is_float():
    assert is_float("1213.6")
    assert not is_float("234324.6 sdfsdf")
    assert not is_float("asdfasdf")
    assert not is_float([])
    assert is_float(1.23123)


def test_sanitize_data_for_sf():
    assert sanitize_data_for_sf("true")
    assert not sanitize_data_for_sf("FalSE")
    assert sanitize_data_for_sf("123") == 123
    assert sanitize_data_for_sf(NaN) == ""
    assert sanitize_data_for_sf([123, "123"]) == "123;123"
    assert sanitize_data_for_sf("asdfasdf") == "asdfasdf"
    now = datetime.now()
    assert sanitize_data_for_sf(now) == now.astimezone().strftime(
        "%Y-%m-%dT%H:%M:%S.000%z"
    )


def test_extract_first_name_last_name():
    assert_frame_equal(
        extract_firstname_lastname(DataFrame({"Name": ["Jimmy Faza"]}), "Name"),
        DataFrame(
            {
                "Name": ["Jimmy Faza"],
                "FirstName": ["Jimmy"],
                "LastName": ["Faza"],
            }
        ),
    )
    assert_frame_equal(
        extract_firstname_lastname(DataFrame({"Name": ["Jimmy s Faza"]}), "Name"),
        DataFrame(
            {
                "Name": ["Jimmy s Faza"],
                "FirstName": ["Jimmy s"],
                "LastName": ["Faza"],
            }
        ),
    )
    assert_frame_equal(
        extract_firstname_lastname(
            DataFrame({"Name": ["Jimmy-marrie s Faza"]}), "Name"
        ),
        DataFrame(
            {
                "Name": ["Jimmy-marrie s Faza"],
                "FirstName": ["Jimmy-marrie s"],
                "LastName": ["Faza"],
            }
        ),
    )
    multi = extract_firstname_lastname(
        DataFrame({"Name": ["Jimmy t Faza", "test"]}), "Name"
    )
    assert_frame_equal(
        multi,
        DataFrame(
            {
                "Name": ["Jimmy t Faza", "test"],
                "FirstName": ["Jimmy t", "test"],
                "LastName": ["Faza", "test"],
            }
        ),
    )


def test_to_sf_datetime():
    assert_series_equal(
        to_sf_datetime(Series(["2021-01-01 12:00:00"])),
        Series(["2021-01-01T12:00:00.000-07:00"]),
    )
    assert_series_equal(
        to_sf_datetime(Series(["02-01-2023"]), "%m-%d-%Y"),
        Series(["2023-02-01T00:00:00.000-07:00"]),
    )


def test_extract_and_modify_numbers():
    # Case 1: Standard case with a mix of 4-digit and 5-digit numbers
    text_series = Series(["order 1234", "product 5678", "item 91011", "code 1213"])
    expected = Series(["01234", "05678", "91011", "01213"])
    result = extract_zipcode(text_series)
    assert_series_equal(result, expected)

    # Case 2: No numbers present
    text_series = Series(["order", "product", "item", "code"])
    expected = Series([None, None, None, None], dtype=object)
    result = extract_zipcode(text_series)
    assert_series_equal(result, expected)

    # Case 3: Mixed content with no 4-digit numbers
    text_series = Series(["order 123", "product 56789", "item 9101", "code 1213"])
    expected = Series(["123", "56789", "09101", "01213"])
    result = extract_zipcode(text_series)
    assert_series_equal(result, expected)

    # Case 4: Empty Series
    text_series = Series([], dtype=object)
    expected = Series([], dtype=object)
    result = extract_zipcode(text_series)
    assert_series_equal(result, expected)

    # Case 5: Series with NaN values
    text_series = Series(["order 1234", None, "item 91011", ""])
    expected = Series(["01234", None, "91011", None], dtype=object)
    result = extract_zipcode(text_series)
    assert_series_equal(result, expected)

    # Case 6: Series with numbers longer than 5 digits
    text_series = Series(["order 123456", "product 567890", "item 12345"])
    expected = Series(["12345", "56789", "12345"])
    result = extract_zipcode(text_series)
    assert_series_equal(result, expected)
