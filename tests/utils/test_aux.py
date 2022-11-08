from omnivore.utils.aux import toSalesforcePhone, toSalesforceEmail, extractId, extract_address, Address


def assert_partial_dict(dict_1, dict_2):
    for key in dict_2:
        if dict_1[key] != dict_2[key]:
            print(f'dict_1 {key} = {dict_1[key]} dict_2 {key} = {dict_2[key]}')
            return False
    return True


def test_toSalesforcePhone():
    # Test extracting number
    clean = toSalesforcePhone('askdfbashdfhsda123dsfasdf444asdfasdf---==\'as4567')
    assert clean == '234444567'

    # Test to remove area code
    no_one = toSalesforcePhone('+1 917 333 3214')
    assert no_one == '9173333214'

    # Test only return 1 phone number that consists of 10 digits
    only_10 = toSalesforcePhone('+1 917 333 3214 +1 917 333 3214 +1 917 333 3214')
    assert len(only_10) == 10

    # Error prove if the input is not string
    nothing = toSalesforcePhone(None)
    assert nothing == ''

    # Error prove if the input is not string
    nothing = toSalesforcePhone({})
    assert nothing == ''

    # Error prove if the input is not string
    nothing = toSalesforcePhone([])
    assert nothing == ''

    # Convert to string
    phone_from_number = toSalesforcePhone(9123348123)
    assert phone_from_number == '9123348123'


def test_toSalesforceEmail():
    # return nothing if no email detected
    nothing = toSalesforceEmail([])
    assert nothing == ''

    # return nothing if no email detected
    nothing = toSalesforceEmail('sdfsdfdsf')
    assert nothing == ''

    # return nothing if no email detected
    nothing = toSalesforceEmail(12121344)
    assert nothing == ''

    # return nothing if no email detected
    nothing = toSalesforceEmail({})
    assert nothing == ''

    # Test multiple email, only get the first one
    first_email = toSalesforceEmail('garbafe@gmail.com   asdfasdfasdf@yahoo.com')
    assert first_email == 'garbafe@gmail.com'

    # Lower all email
    lower_email = toSalesforceEmail('JffsImmY@yaHOO.com')
    assert lower_email == 'jffsimmy@yahoo.com'


def test_extractId():
    # Return empty array when no id
    nothing = extractId('asdfasdfasdfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsfsf')
    assert nothing == []

    # ID shouldn't be less than 21 char
    no_id = extractId('<|>333ffffsf')
    assert no_id == []

    # Test extracting from encoded url
    decoded = extractId('%3C%7C%3E00Q3i00000E6fVKEAZ%3C%7C%3E%20test%20test%20%3C%7C%3E00Q3i00000E6fVKEAZ%3C%7C%3E')
    assert decoded == ['00Q3i00000E6fVKEAZ', '00Q3i00000E6fVKEAZ']

    # From a paragraph
    parag = extractId('test test \n \t \n <|>00Q3i00000E6fVKEAZ<|>sdfsdf this is just a test')
    assert parag == ['00Q3i00000E6fVKEAZ']


def test_extract_address():
    # Neeeco
    assert assert_partial_dict(extract_address("57 Codman Hill Avenue Boston, MA 02124    (617) 448-8742"),
                               Address(street='57 Codman Hill Avenue', city='Boston', zipcode='02124'))
    assert assert_partial_dict(extract_address("6 West Bedford Street Methuen, MA 01844      ("),
                               Address(street='6 West Bedford Street', city='Methuen', zipcode='01844'))
    assert assert_partial_dict(extract_address("212 Sycamore St, Unit 2, Watertown, MA 02472"), Address(
        street='212 Sycamore St', city='Watertown', unit='Unit 2', zipcode='02472'))
    assert assert_partial_dict(extract_address('''406 hollis st
01702'''), Address(street='406 hollis st', zipcode='01702'))
    assert assert_partial_dict(extract_address("22 Saxonia Ave Unit 1 Lawrence, MA 01841"), Address(
        street='22 Saxonia Ave', unit='Unit 1', city='Lawrence', zipcode='01841'))
    assert assert_partial_dict(extract_address('696 Bennington St, Unit 1, East Boston MA 02128'),
                               Address(street='696 Bennington St', unit='Unit 1', zipcode='02128'))

    # Homeworks
    assert assert_partial_dict(extract_address("(Unit 1) 117 Malden Street"),
                              Address(street='117 Malden Street', unit='(Unit 1)'))
    assert assert_partial_dict(extract_address("212 Webster St Malden unit 2"),
                              Address(street='212 Webster St', unit='unit 2'))
    assert assert_partial_dict(extract_address("(Unit B) 1084 Washington Street"),
                              Address(street='1084 Washington Street', unit='(Unit B)'))
    assert assert_partial_dict(extract_address('202 1/2 Bridge Street'), Address(street='202 1/2 Bridge Street'))
    assert assert_partial_dict(extract_address("Unit(2) 36 Lothrop Street"), Address(
        street='36 Lothrop Street', unit='Unit (2)'))
    assert assert_partial_dict(extract_address('153 Mount Vernon Street'),
                              Address(street='153 Mount Vernon Street'))
    assert assert_partial_dict(extract_address('(Unit 15 (1)) 15 Pierce Avenue'),
                              Address(street='15 Pierce Avenue', unit='(Unit 15 (1))'))
# VHI
    assert assert_partial_dict(extract_address("99 Ferry St #3,"),
                              Address(street='99 Ferry St', unit='# 3'))
    assert assert_partial_dict(extract_address("6 Nollet Drive AnDOVER"),
                              Address(street='6 Nollet Drive', city='AnDOVER'))
    assert assert_partial_dict(extract_address("3 Pearl Avenue, unit 2"),
                              Address(street='3 Pearl Avenue', unit='unit 2'))
    assert assert_partial_dict(extract_address('28 BUSWELL ST FL 2,'), Address(street='28 BUSWELL ST', unit='FL 2'))
# Revise