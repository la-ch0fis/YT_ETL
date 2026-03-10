def test_api_key(api_key):
    """
        We create the function for the testing an it has to start with the prefix 'test_'
        and an appropiate name, in this case 'api_key'. This function will take as argument the API_KEY fixture defined in the conftest.py file.
    """
    assert api_key == "MOCK_KEY1234"
