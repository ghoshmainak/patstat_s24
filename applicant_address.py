import pandas as pd
from config import DATA_FOLDER


DATE_NAN = '9999-12-31'


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


def get_applicant_addresses(applicants, person_data):
    """
    Merge applicant data with person address data.

    Args:
        applicants (pd.DataFrame): DataFrame containing applicant information.
        person_data (pd.DataFrame): DataFrame containing person addresses and
        country codes.

    Returns:
        pd.DataFrame: Merged DataFrame containing applicant information along
        with their addresses.
    """
    # Merge applicant data with person addresses
    applicant_addresses = applicants.merge(person_data, on='person_id',
                                           how='left')
    return applicant_addresses.sort_values(['appln_id', 'applt_seq_nr'])


def get_priority_applicant_addresses(priority_data, applicant_addresses):
    """
    Retrieve the applicant addresses for the earliest priority applications.

    Args:
        priority_data (pd.DataFrame): DataFrame containing earliest priority
        application data.
        applicant_addresses (pd.DataFrame): DataFrame with applicant addresses.

    Returns:
        pd.DataFrame: DataFrame of applicant addresses for earliest priority
        applications.
    """
    # Merge priority applications with applicant addresses
    addresses_tmp = applicant_addresses.rename(
        columns={'appln_id': 'earliest_prior_appln_id'})
    result = priority_data.merge(addresses_tmp, on='earliest_prior_appln_id',
                                 how='left')
    result.drop(['appln_id'], axis=1, inplace=True)
    result.drop_duplicates(inplace=True)
    return result


def get_appln_no_applicants(priority_filings):
    """
    Retrieve applications that are not associated with any applicant.

    Args:
        priority_filings (pd.DataFrame): DataFrame containing earliest priority applications 
                                         along with the applicants' addresses.

    Returns:
        pd.DataFrame: DataFrame of applications without applicants.
    """
    # Retain original columns
    cols = ['earliest_prior_appln_id', 'earliest_priority_date', 'earliest_prior_appln_auth']
    
    # Create a copy to avoid modifying the original DataFrame
    priority_filings_copy = priority_filings.copy()

    # Mark rows where applicant address exists
    priority_filings_copy['address_exists'] = priority_filings_copy['person_address'].notna()

    # Count how many addresses exist for each priority application
    priority_filings_copy['address_exists_count'] = priority_filings_copy.groupby('earliest_prior_appln_id')['address_exists'].transform('sum')

    # Filter applications where no applicant address exists
    no_applt_applns = priority_filings_copy.query("address_exists_count == 0")[cols]

    return no_applt_applns.drop_duplicates()


def replenish_address_pct(priority_no_applt_address, applt_addresses, tls201):
    """
    Replenish missing applicant addresses for priority filings from other PCT linked filings.

    Args:
        priority_no_applt_address (pd.DataFrame): DataFrame containing priority applications without applicant addresses.
        applt_addresses (pd.DataFrame): DataFrame containing applicant addresses.
        tls201: (pd.DataFrame): DataFrame consisting of appln detail
    Returns:
        pd.DataFrame: Updated DataFrame with replenished applicant addresses where possible.
    """
    # Load PCT families
    pct_families = pd.read_stata(DATA_FOLDER / "PCT_families.dta")
    # Add address to PCT family members
    pct_families_w_address = pct_families.merge(applt_addresses,
                                                on='appln_id')
    # Mark rows where applicant address exists
    pct_families_w_address['address_exists'] = pct_families_w_address['person_address'].notna()

    # Count how many addresses exist for each priority application
    pct_families_w_address['address_exists_count'] = pct_families_w_address.groupby('appln_id')['address_exists'].transform('sum')

    # Filter applications where applicant address exists
    pct_families_w_address = pct_families_w_address.query("address_exists_count > 0").copy()
    pct_families_w_address.drop(['address_exists', 'address_exists_count'],
                                axis=1, inplace=True)
    
    # Add filing date to PCT family members
    pct_families_w_address = pct_families_w_address.merge(tls201, on='appln_id')

    # as multiple members in PCT family can have intentor addresses,
    # we need a priority rule to pick one member of the PCT family
    # so, pick earliest filed one. If there still exists multiple,
    # pick one with smallest application ID
    pct_families_w_address['earliest'] = pct_families_w_address.groupby('WO_appln_id')['appln_filing_date'].rank('dense')
    pct_families_w_address = pct_families_w_address[pct_families_w_address['earliest'] == 1].drop(
        ['earliest', 'appln_filing_date'], axis=1
    ).copy()
    pct_families_w_address['earliest'] = pct_families_w_address.groupby('WO_appln_id')['appln_id'].rank('dense')
    pct_families_w_address = pct_families_w_address[pct_families_w_address['earliest'] == 1].drop(
        ['earliest'], axis=1
    ).copy()

    # Retrieve WO appln ID
    priority_no_applt_address = priority_no_applt_address.merge(pct_families.rename(columns={
        'appln_id': 'earliest_prior_appln_id'
    }), on='earliest_prior_appln_id')

    # Pull up other members having same WO appln ID and applicant addresses
    priority_no_applt_address = priority_no_applt_address.merge(
        pct_families_w_address, on='WO_appln_id'
    )

    priority_no_applt_address.drop(['WO_appln_id'],
                                    axis=1,
                                    inplace=True)
    priority_no_applt_address.rename(
        columns={'appln_id': 'replenished_by_appln_id'}, inplace=True
    )
    
    return priority_no_applt_address


def replenish_address_equivalents(priority_no_applt_address, applt_addresses, tls201):
    """
    Replenish missing applicant addresses for priority filings from equivalent filings.

    Args:
        priority_no_applt_address (pd.DataFrame): DataFrame containing priority applications without applicant addresses.
        applt_addresses (pd.DataFrame): DataFrame containing applicant addresses.
        tls201: (pd.DataFrame): DataFrame consisting of appln detail
    Returns:
        pd.DataFrame: Updated DataFrame with replenished applicant addresses where possible.
    """
    # Load equivalent patent application data
    equivalents = pd.read_feather(DATA_FOLDER / "patent_equivalents.feather")
    # Add applicant address info to the equivalent applications
    equivalents_w_address = equivalents.merge(applt_addresses, on='appln_id')
    # Drop if an equivalent does not have address
    equivalents_w_address['add_exist'] = equivalents_w_address['person_address'].notna()
    equivalents_w_address['add_count'] = equivalents_w_address.groupby('appln_id')['add_exist'].transform('sum')
    equivalents_w_address = equivalents_w_address[equivalents_w_address['add_count'] > 0].copy()
    equivalents_w_address.drop(['add_exist', 'add_count'], axis=1, inplace=True)

    # add filing date
    equivalents_w_address = equivalents_w_address.merge(tls201, on='appln_id')

    # Pick earliest filed one per equivalent group
    equivalents_w_address['earliest'] = equivalents_w_address.groupby('eqv_grp_num')['appln_filing_date'].rank('dense')
    equivalents_w_address = equivalents_w_address[equivalents_w_address['earliest'] == 1].drop(['earliest', 'appln_filing_date'], axis=1).copy()
    equivalents_w_address['earliest'] = equivalents_w_address.groupby('eqv_grp_num')['appln_id'].rank('dense')
    equivalents_w_address = equivalents_w_address[equivalents_w_address['earliest'] == 1].drop(['earliest'], axis=1).copy()

    # Merge priority filings (missing applicant addresses) with equivalent filings
    priority_filing_group_no = priority_no_applt_address.merge(equivalents,
                                                               left_on='earliest_prior_appln_id',
                                                               right_on='appln_id')
    priority_filing_group_no.drop('appln_id', axis=1, inplace=True)
    # Merge with equivalents that have applicant addresses
    priority_filing_equivalents = priority_filing_group_no.merge(equivalents_w_address,
                                                                 on='eqv_grp_num')
    priority_filing_equivalents.drop('eqv_grp_num', axis=1, inplace=True)
    
    priority_filing_equivalents.rename(columns={'appln_id': 'replenished_by_appln_id'}, inplace=True)

    return priority_filing_equivalents


def get_docdb_level_applt_address(applt_addresses, tls201):
    """
    Find applicant address for earliest filed application within a DOCDB family
    """
    appln_docdb = pd.read_stata(DATA_FOLDER / "appln_docdb_number.dta")
    appln_docdb = appln_docdb.merge(tls201, on='appln_id')
    appln_docdb = appln_docdb.merge(applt_addresses, on='appln_id')
    # Drop if an equivalent does not have address
    appln_docdb['add_exist'] = appln_docdb['person_address'].notna()
    appln_docdb['add_count'] = appln_docdb.groupby('appln_id')['add_exist'].transform('sum')
    appln_docdb = appln_docdb[appln_docdb['add_count'] > 0].copy()
    appln_docdb.drop(['add_exist', 'add_count'], axis=1, inplace=True)

    appln_docdb['earliest'] = appln_docdb.groupby('docdb_family_id')['appln_filing_date'].rank('dense')
    appln_docdb = appln_docdb[appln_docdb['earliest'] == 1].drop(['earliest', 'appln_filing_date'], axis=1).copy()
    appln_docdb['earliest'] = appln_docdb.groupby('docdb_family_id')['appln_id'].rank('dense')
    appln_docdb = appln_docdb[appln_docdb['earliest'] == 1].drop(['earliest'], axis=1).copy()

    return appln_docdb


if __name__ == '__main__':
    # Load TLS201 (appln_id and appln_filing_date)   
    tls201 = pd.read_feather(DATA_FOLDER / "TLS201.feather", columns=['appln_id', 'appln_filing_date'])
    tls201['appln_filing_date'] = tls201['appln_filing_date'].fillna(DATE_NAN)

    # Load the data
    applicants = pd.read_stata(DATA_FOLDER / "applicants_TLS207.dta")

    # Clean and load the person data (addresses and country codes)
    person_data = load_and_clean_person_data()

    # Retrieve applicant addresses
    applicant_addresses = get_applicant_addresses(applicants, person_data)

    # Ensure shape consistency
    assert applicants.shape[0] == applicant_addresses.shape[0], "Mismatch in applicant data after merge!"
    applicant_addresses.to_feather(
        DATA_FOLDER / "applicants_addresses.feather")
    
    # Get applicant addresses for earliest priority applications
    # and save to the disk
    earliest_priority = pd.read_stata(DATA_FOLDER / "earliest_priority.dta")
    priority_applicant_address = get_priority_applicant_addresses(
        earliest_priority, applicant_addresses)
    priority_applicant_address.to_feather(
        DATA_FOLDER / "priority_applicants_addresses.feather")
    assert priority_applicant_address['earliest_prior_appln_id'].nunique() == \
        earliest_priority['earliest_prior_appln_id'].nunique(), "Mismatch in count of priority filing after merge!"
    
    # Identify priority filings that do not have any applicant addresses
    priority_no_applicant_address = get_appln_no_applicants(priority_applicant_address)

    # Replenish applicant addresses from other PCT linked application
    priority_w_address_replenished_pct = replenish_address_pct(priority_no_applicant_address,
                                                               applicant_addresses,
                                                               tls201)
    
    # Remove priority filings that have been replenished
    deleted = priority_no_applicant_address['earliest_prior_appln_id'].isin(
        priority_w_address_replenished_pct['earliest_prior_appln_id']
    )
    priority_no_applicant_address = priority_no_applicant_address[~deleted].copy()

    # Replenish applicant addresses from equivalents
    priority_w_address_replenished_equiv = replenish_address_equivalents(priority_no_applicant_address,
                                                                         applicant_addresses,
                                                                         tls201)
    
    # combine replenished address
    priority_w_address_replenished = pd.concat(
        [priority_w_address_replenished_pct, priority_w_address_replenished_equiv],
        ignore_index=True
    )

    # Remove priority filings that have been replenished
    deleted = priority_applicant_address['earliest_prior_appln_id'].isin(
        priority_w_address_replenished['earliest_prior_appln_id']
    )
    priority_applicant_address = priority_applicant_address[~deleted].copy()
    # Combine the original and replenished applicant addresses
    full_priority_applicant_address = pd.concat(
        [priority_applicant_address, priority_w_address_replenished],
        ignore_index=True
    )

    assert full_priority_applicant_address['earliest_prior_appln_id'].nunique() == \
        earliest_priority['earliest_prior_appln_id'].nunique(), "Mismatch in count of priority filing after address replenishment!"

    # Save the final result with replenished applicant addresses
    full_priority_applicant_address.to_feather(
        DATA_FOLDER / "priority_applicants_addresses.feather"
    )

    # Applicant addresses for earliest filed application per DOCDB family 
    docdb_level_applt_address = get_docdb_level_applt_address(
        applicant_addresses,
        tls201
    )
    docdb_level_applt_address.to_feather(DATA_FOLDER / "DOCDB_earliest_file_applicants_addresses.feather")

