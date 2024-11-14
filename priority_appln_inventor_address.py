"""
Find the addresses of the inventors of a priority application.
"""
import pandas as pd
from config import DATA_FOLDER


def empty_to_null_string(text):
    """
    Convert empty strings to None.

    Args:
        text (str): The input text to be converted.

    Returns:
        str or None: Returns None if the input is an empty string, 
                     otherwise returns the original text.
    """
    return None if text == '' else text


def load_and_clean_person_data():
    """
    Load and clean person address and country code data.
    
    Returns:
        pd.DataFrame: Cleaned DataFrame containing person_id, person_address, and person_ctry_code.
    """
    address = pd.read_feather(DATA_FOLDER / "person_address.feather")
    ctry_code = pd.read_stata(DATA_FOLDER / "person_ctry_code.dta")

    # Apply conversion of empty strings to None
    address['person_address'] = address['person_address'].map(
        empty_to_null_string)
    ctry_code['person_ctry_code'] = ctry_code['person_ctry_code'].map(
        empty_to_null_string)

    # Merge address and country code, drop rows with both missing
    person_data = address.merge(ctry_code, on='person_id', how='outer')
    assert person_data.person_id.duplicated().sum() == 0  # Ensure no duplicate person IDs
    person_data.dropna(subset=['person_address', 'person_ctry_code'], how='all',
                       inplace=True)

    return person_data


def get_inventor_addresses(inventors, person_data):
    """
    Merge inventor data with person address data.

    Args:
        inventors (pd.DataFrame): DataFrame containing inventor information.
        person_data (pd.DataFrame): DataFrame containing person addresses and
        country codes.

    Returns:
        pd.DataFrame: Merged DataFrame containing inventor information along
        with their addresses.
    """
    # Merge inventor data with person addresses
    inventor_addresses = inventors.merge(person_data, on='person_id',
                                         how='left')
    return inventor_addresses.sort_values(['appln_id', 'invt_seq_nr'])


def get_priority_inventor_addresses(priority_data, inventor_addresses):
    """
    Retrieve the inventor addresses for the earliest priority applications.

    Args:
        priority_data (pd.DataFrame): DataFrame containing earliest priority
        application data.
        inventor_addresses (pd.DataFrame): DataFrame with inventor addresses.

    Returns:
        pd.DataFrame: DataFrame of inventor addresses for earliest priority
        applications.
    """
    # Merge priority applications with inventor addresses
    addresses_tmp = inventor_addresses.rename(
        columns={'appln_id': 'earliest_prior_appln_id'})
    result = priority_data.merge(addresses_tmp, on='earliest_prior_appln_id',
                                 how='left')
    result.drop(['appln_id'], axis=1, inplace=True)
    result.drop_duplicates(inplace=True)
    return result


if __name__ == '__main__':
    # Load the data
    earliest_priority = pd.read_stata(DATA_FOLDER / "earliest_priority.dta")
    inventors = pd.read_stata(DATA_FOLDER / "inventors_TLS207.dta")

    # Clean and load the person data (addresses and country codes)
    person_data = load_and_clean_person_data()

    # Retrieve inventor addresses
    inventor_addresses = get_inventor_addresses(inventors, person_data)

    # Ensure shape consistency
    assert inventors.shape[0] == inventor_addresses.shape[0], "Mismatch in inventor data after merge!"

    # Get inventor addresses for earliest priority applications
    # and save to the disk
    priority_inventor_address = get_priority_inventor_addresses(
        earliest_priority, inventor_addresses)
    priority_inventor_address.to_feather(
        DATA_FOLDER / "priority_inventors_addresses.feather")
    
    assert priority_inventor_address['earliest_prior_appln_id'].nunique() == \
        earliest_priority['earliest_prior_appln_id'].nunique(), "Mismatch in count of priority filing after merge!"
    
    address_missing = priority_inventor_address['person_address'].isna()

