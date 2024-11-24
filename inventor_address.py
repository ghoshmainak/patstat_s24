"""
Find the addresses of the inventors of a priority application.
"""
from pathlib import Path
import pandas as pd
from config import DATA_FOLDER


DATE_NAN = '9999-12-31'
ORBIS_IP_PATSTAT_MATCH = Path(r"E:\PERSONS\Mainak Ghosh\Orbis_PATSTAT_match\Data")


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
    # Drop rows where both address and country code are missing
    person_data.dropna(subset=['person_address', 'person_ctry_code'], how='all',
                       inplace=True)

    return person_data


def load_orbis_ip_pat_inventor():
    """
    Load and transform Orbis IP patent inventors
    """
    columns_needed = ['PatPublNr', 'party_address_part1', 'party_city', 'party_state', 'party_postcode', 'party_country', 'RoleNbr', 'RolePos']
    orbisip_patinvt = pd.read_feather(ORBIS_IP_PATSTAT_MATCH / "OrbisIP_Patent_Paties.feather", columns=columns_needed)
    orbisip_patinvt = orbisip_patinvt.query("RoleNbr=='-1'").copy()
    orbisip_patinvt.drop('RoleNbr', axis=1, inplace=True)
    for col in ['party_address_part1', 'party_city', 'party_state', 'party_postcode', 'party_country']:
        orbisip_patinvt[col] = orbisip_patinvt[col].str.strip()
    orbisip_patinvt['person_address'] = orbisip_patinvt['party_address_part1'] + ' ' + orbisip_patinvt['party_city'] + ' ' + orbisip_patinvt['party_state'] + ' ' + orbisip_patinvt['party_postcode']
    orbisip_patinvt['person_address'] = orbisip_patinvt['person_address'].str.strip() 
    orbisip_patinvt['person_address'] = orbisip_patinvt['person_address'].map(empty_to_null_string)
    orbisip_patinvt['party_country'] = orbisip_patinvt['party_country'].map(empty_to_null_string)
    orbisip_patinvt = orbisip_patinvt.dropna(subset=['person_address', 'party_country']).drop(['party_address_part1', 'party_city', 'party_state', 'party_postcode'], axis=1).copy()
    orbisip_patinvt.rename(columns={'party_country': 'person_ctry_code', 'RolePos': 'invt_seq_nr', 'PatPublNr': 'patpublnr'}, inplace=True)

    orbis_patstat_match = pd.read_feather(ORBIS_IP_PATSTAT_MATCH / "OrbisIP_patstat_matched_0.feather",
                                          columns=['patpublnr', 'appln_id', 'publn_date'])
    orbis_patstat_match = orbis_patstat_match.dropna(subset=['patpublnr', 'appln_id']).drop_duplicates()
    orbis_patstat_match['publn_date'] = orbis_patstat_match['publn_date'].fillna(DATE_NAN)
    orbis_patstat_match = orbis_patstat_match.sort_values(['appln_id', 'publn_date', 'patpublnr'])
    orbis_patstat_match['priority'] = orbis_patstat_match.groupby('appln_id').cumcount() + 1
    orbis_patstat_match.drop('publn_date', axis=1, inplace=True)

    orbisip_patinvt = orbisip_patinvt.merge(orbis_patstat_match, on='patpublnr')
    orbisip_patinvt['highest_priority'] = orbisip_patinvt.groupby('appln_id')['priority'].transform('min')
    orbisip_patinvt = orbisip_patinvt.query("highest_priority == priority").copy()

    return orbisip_patinvt.drop(['patpublnr', 'priority', 'highest_priority'], axis=1)


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


def merge_address_from_both_worlds(patstat_invt_addr, orbis_ip_invt_addr):
    """
    Merge patstat inventor address with orbis ip
    """
    # Create a copy to avoid modifying the original DataFrame
    patstat_invt_addr_copy = patstat_invt_addr.copy()

    # Mark rows where inventor address exists
    patstat_invt_addr_copy['address_exists'] = patstat_invt_addr_copy['person_address'].notna()

    # Count how many addresses exist for each application
    patstat_invt_addr_copy['address_exists_count'] = patstat_invt_addr_copy.groupby('appln_id')['address_exists'].transform('sum')

    need_from_orbis = patstat_invt_addr_copy[patstat_invt_addr_copy['address_exists_count'] == 0].copy()[['appln_id']].drop_duplicates()
    need_from_orbis = need_from_orbis.merge(orbis_ip_invt_addr, on='appln_id')

    deleted = patstat_invt_addr_copy.appln_id.isin(
        need_from_orbis.appln_id
    )

    patstat_invt_addr_copy = patstat_invt_addr_copy[~deleted].copy()
    patstat_invt_addr_copy = patstat_invt_addr_copy.drop(['address_exists', 'address_exists_count'], axis=1)

    return pd.concat([patstat_invt_addr_copy, need_from_orbis], ignore_index=True)


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


def get_appln_no_inventors(priority_filings):
    """
    Retrieve applications that are not associated with any inventor.

    Args:
        priority_filings (pd.DataFrame): DataFrame containing earliest priority applications 
                                         along with the inventors' addresses.

    Returns:
        pd.DataFrame: DataFrame of applications without inventors.
    """
    # Retain original columns
    cols = ['earliest_prior_appln_id', 'earliest_priority_date', 'earliest_prior_appln_auth']
    
    # Create a copy to avoid modifying the original DataFrame
    priority_filings_copy = priority_filings.copy()

    # Mark rows where inventor address exists
    priority_filings_copy['address_exists'] = priority_filings_copy['person_address'].notna()

    # Count how many addresses exist for each priority application
    priority_filings_copy['address_exists_count'] = priority_filings_copy.groupby('earliest_prior_appln_id')['address_exists'].transform('sum')

    # Filter applications where no inventor address exists
    no_inventor_applns = priority_filings_copy.query("address_exists_count == 0")[cols]

    return no_inventor_applns.drop_duplicates()


def construct_equivalent_w_address(inventor_addresses, equivalents, tls201):
    """
    Fetch equivalents with addresses and filing date
    Args:
        inventor_addresses (pd.DataFrame): DataFrame containing inventor addresses.
        equivalents (pd.DataFrame): DataFrame containing equivalents
        tls201: (pd.DataFrame): DataFrame consisting of appln detail

    Returns:
        pd.DataFrame: Equivalents with inventor addresses and filing date.
    """
    # add filing date
    obscheck = equivalents.shape[0]
    equivalents = equivalents.merge(tls201, on='appln_id')
    assert equivalents.shape[0] == obscheck
    # add inventor addresses
    equivalents = equivalents.merge(inventor_addresses, on='appln_id', how='left')
    # Mark rows where inventor address exists
    equivalents['address_exists'] = equivalents['person_address'].notna()

    # Count how many addresses exist for each priority application
    equivalents['address_exists_count'] = equivalents.groupby('appln_id')['address_exists'].transform('sum')

    # Filter applications where inventor address exists
    equivalents = equivalents.query("address_exists_count > 0").copy()
    return equivalents.drop(['address_exists', 'address_exists_count'], axis=1)


def get_earliest_equivalent_with_address(equivalents):
    """
    Helper function to find the earliest equivalent application with an inventor address.

    Args:
        equivalents (pd.DataFrame): DataFrame containing equivalent patent applications.

    Returns:
        pd.DataFrame: DataFrame with the earliest equivalent inventor address.
    """
    equivalents.sort_values(['earliest_prior_appln_id', 'appln_filing_date', 'appln_id'], inplace=True)
    equivalents['rank'] = equivalents.groupby('earliest_prior_appln_id').cumcount() + 1
    return equivalents.query("rank == 1").drop(['appln_filing_date', 'rank'], axis=1).copy()


def replenish_address_pct(priority_no_inventor_address, inventor_addresses, tls201):
    """
    Replenish missing inventor addresses for priority filings from other PCT linked filings.

    Args:
        priority_no_inventor_address (pd.DataFrame): DataFrame containing priority applications without inventor addresses.
        inventor_addresses (pd.DataFrame): DataFrame containing inventor addresses.
        tls201: (pd.DataFrame): DataFrame consisting of appln detail
    Returns:
        pd.DataFrame: Updated DataFrame with replenished inventor addresses where possible.
    """
    # Load PCT families
    pct_families = pd.read_stata(DATA_FOLDER / "PCT_families.dta")
    # Add address to PCT family members
    pct_families_w_address = pct_families.merge(inventor_addresses,
                                                on='appln_id')
    # Mark rows where inventor address exists
    pct_families_w_address['address_exists'] = pct_families_w_address['person_address'].notna()

    # Count how many addresses exist for each priority application
    pct_families_w_address['address_exists_count'] = pct_families_w_address.groupby('appln_id')['address_exists'].transform('sum')

    # Filter applications where inventor address exists
    pct_families_w_address = pct_families_w_address.query("address_exists_count > 0").copy()
    pct_families_w_address.drop(['address_exists', 'address_exists_count'],
                                axis=1, inplace=True)
    
    # Add filing date to PCT family members
    pct_families_w_address = pct_families_w_address.merge(tls201, on='appln_id')

    # Retrieve WO appln ID
    priority_no_inventor_address = priority_no_inventor_address.merge(pct_families.rename(columns={
        'appln_id': 'earliest_prior_appln_id'
    }), on='earliest_prior_appln_id')

    # Pull up other members having same WO appln ID and inventor addresses
    priority_no_inventor_address = priority_no_inventor_address.merge(
        pct_families_w_address, on='WO_appln_id'
    )

    # as multiple members in PCT family can have intentor addresses,
    # we need a priority rule to pick one member of the PCT family
    # so, pick earliest filed one. If there still exists multiple,
    # pick one with smallest application ID
    priority_no_inventor_address['appln_filing_date'] = priority_no_inventor_address['appln_filing_date'].fillna(DATE_NAN)
    earliest_member_w_address = get_earliest_equivalent_with_address(
        priority_no_inventor_address[['earliest_prior_appln_id', 'appln_id', 'appln_filing_date']].drop_duplicates()
    )
    priority_no_inventor_address = priority_no_inventor_address.merge(
        earliest_member_w_address, on=['earliest_prior_appln_id', 'appln_id']
    )
    priority_no_inventor_address.drop(['appln_filing_date', 'WO_appln_id'],
                                      axis=1,
                                      inplace=True)
    priority_no_inventor_address.rename(
        columns={'appln_id': 'replenished_by_appln_id'}, inplace=True
    )
    
    return priority_no_inventor_address


def replenish_address_equivalents(priority_no_inventor_address, inventor_addresses, tls201):
    """
    Replenish missing inventor addresses for priority filings from equivalent filings.

    Args:
        priority_no_inventor_address (pd.DataFrame): DataFrame containing priority applications without inventor addresses.
        inventor_addresses (pd.DataFrame): DataFrame containing inventor addresses.
        tls201: (pd.DataFrame): DataFrame consisting of appln detail
    Returns:
        pd.DataFrame: Updated DataFrame with replenished inventor addresses where possible.
    """
    # Load equivalent patent application data
    equivalents = pd.read_feather(DATA_FOLDER / "patent_equivalents.feather")
    # Add inventor address info to the equivalent applications
    equivalents_w_address = construct_equivalent_w_address(inventor_addresses,
                                                           equivalents,
                                                           tls201)
    # Merge priority filings (missing inventor addresses) with equivalent filings
    priority_filing_group_no = priority_no_inventor_address.merge(equivalents,
                                                                  left_on='earliest_prior_appln_id',
                                                                  right_on='appln_id')
    priority_filing_group_no.drop('appln_id', axis=1, inplace=True)
    # Merge with equivalents that have inventor addresses
    priority_filing_equivalents = priority_filing_group_no.merge(equivalents_w_address,
                                                                 on='eqv_grp_num')
    priority_filing_equivalents.drop('eqv_grp_num', axis=1, inplace=True)
    
    # Fill any missing filing dates
    priority_filing_equivalents['appln_filing_date'] = priority_filing_equivalents['appln_filing_date'].fillna(DATE_NAN)
    
    # Get the earliest equivalent with an inventor address for each priority application
    earliest_equivalent_w_address = get_earliest_equivalent_with_address(priority_filing_equivalents[['earliest_prior_appln_id', 'appln_id', 'appln_filing_date']].drop_duplicates())
    priority_filing_equivalents = priority_filing_equivalents.merge(earliest_equivalent_w_address,
                                                                    on=['earliest_prior_appln_id', 'appln_id'])
    priority_filing_equivalents = priority_filing_equivalents.drop('appln_filing_date', axis=1)
    priority_filing_equivalents.rename(columns={'appln_id': 'replenished_by_appln_id'}, inplace=True)
    return priority_filing_equivalents


if __name__ == '__main__':
    # Load TLS201 (appln_id and appln_filing_date)   
    tls201 = pd.read_feather(DATA_FOLDER / "TLS201.feather", columns=['appln_id', 'appln_filing_date'])

    # Load the data
    earliest_priority = pd.read_stata(DATA_FOLDER / "earliest_priority.dta")
    inventors = pd.read_stata(DATA_FOLDER / "inventors_TLS207.dta")

    # Clean and load the person data (addresses and country codes)
    person_data = load_and_clean_person_data()

    # Retrieve inventor addresses
    inventor_addresses = get_inventor_addresses(inventors, person_data)
    inventor_addresses['source'] = 'patstat'

    # Load patent inventor address from Orbis IP
    orbisip_pat_invt = load_orbis_ip_pat_inventor()
    orbisip_pat_invt['source'] = 'orbis_ip'
    orbisip_pat_invt['invt_seq_nr'] = orbisip_pat_invt['invt_seq_nr'].astype(int)

    # merge patstat inventor address and Orbis IP inventor address
    inventor_addresses = merge_address_from_both_worlds(inventor_addresses,
                                                        orbisip_pat_invt)

    inventor_addresses.to_feather(
        DATA_FOLDER / "inventors_addresses.feather")

    # Get inventor addresses for earliest priority applications
    # and save to the disk
    priority_inventor_address = get_priority_inventor_addresses(
        earliest_priority, inventor_addresses)
    assert priority_inventor_address['earliest_prior_appln_id'].nunique() == \
        earliest_priority['earliest_prior_appln_id'].nunique(), "Mismatch in count of priority filing after merge!"
    priority_inventor_address.to_feather(
        DATA_FOLDER / "priority_inventors_addresses.feather")
    
    # Identify priority filings that do not have any inventor addresses
    priority_no_inventor_address = get_appln_no_inventors(priority_inventor_address)

    # Replenish inventor addresses from other PCT linked application
    priority_w_address_replenished_pct = replenish_address_pct(priority_no_inventor_address,
                                                               inventor_addresses,
                                                               tls201)
    
    # Remove priority filings that have been replenished
    deleted = priority_no_inventor_address['earliest_prior_appln_id'].isin(
        priority_w_address_replenished_pct['earliest_prior_appln_id']
    )
    priority_no_inventor_address = priority_no_inventor_address[~deleted].copy()

    # Replenish inventor addresses from equivalents
    priority_w_address_replenished_equiv = replenish_address_equivalents(priority_no_inventor_address,
                                                                         inventor_addresses,
                                                                         tls201)
    
    # combine replenished address
    priority_w_address_replenished = pd.concat(
        [priority_w_address_replenished_pct, priority_w_address_replenished_equiv],
        ignore_index=True
    )

    # Remove priority filings that have been replenished
    deleted = priority_inventor_address['earliest_prior_appln_id'].isin(
        priority_w_address_replenished['earliest_prior_appln_id']
    )
    priority_inventor_address = priority_inventor_address[~deleted]

    # Combine the original and replenished inventor addresses
    full_priority_inventor_address = pd.concat(
        [priority_inventor_address, priority_w_address_replenished],
        ignore_index=True
    )

    assert full_priority_inventor_address['earliest_prior_appln_id'].nunique() == \
        earliest_priority['earliest_prior_appln_id'].nunique(), "Mismatch in count of priority filing after address replenishment!"

    # Save the final result with replenished inventor addresses
    full_priority_inventor_address.to_feather(
        DATA_FOLDER / "priority_inventors_addresses.feather"
    )
