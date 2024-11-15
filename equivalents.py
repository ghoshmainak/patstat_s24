"""
Drawing equivalents following the section 7 of the following paper:
Martinez, C. (2010), "Insight into Different Types of Patent Families", 
OECD Science, Technology and Industry Working Papers, No. 2010/02, 
OECD Publishing, Paris, https://doi.org/10.1787/5kml97dr6ptl-en.
"""
import pandas as pd
from config import DATA_FOLDER


# Define constants
TLS204_PATH = DATA_FOLDER / "TLS204.feather"


def conv_list_int_string(list_int):
    """
    Convert a list of integers to a pipe-separated string.

    Args:
        list_int (list of int): List of integers to convert.

    Returns:
        str: Pipe-separated string of integers.
    """
    return '|'.join(map(str, list_int))


def load_and_process_priorities():
    """
    Load priority data and combine priorities into lists per application.

    Returns:
        pd.DataFrame: DataFrame with priority applications combined and sorted.
    """
    # Load the priorities data
    tls_204 = pd.read_feather(TLS204_PATH)

    # Group by 'appln_id' and aggregate 'prior_appln_id' into a sorted list
    priority_combined = tls_204.groupby('appln_id')['prior_appln_id'].agg(list).reset_index()

    # Sort the priority application IDs in each list to maintain consistency
    priority_combined['prior_appln_id'] = priority_combined['prior_appln_id'].map(sorted)

    # Convert the list of priority IDs to a pipe-separated string
    priority_combined['prior_appln_id_str'] = priority_combined['prior_appln_id'].map(conv_list_int_string)

    return priority_combined


def contains_pipe(input_string):
    """
    Check if the input string contains the pipe symbol '|'.

    Args:
        input_string (str): The string to check.

    Returns:
        bool: True if the string contains '|', otherwise False.
    """
    return '|' in input_string


def create_equivalents(df):
    """
    Create equivalent groups of applications
    Args:
        df (pd.DataFrame): DataFrame with 'appln_id' and 'prior_appln_id_str'.

    Returns:
        list: List of sets, where each set contains equivalent application IDs.
    """
    equivalent_groups = []
    for _, row in df.iterrows():
        equiv = set(row['appln_id'])
        if not contains_pipe(row['prior_appln_id_str']):
            equiv.add(int(row['prior_appln_id_str']))
        equivalent_groups.append(equiv)
    return equivalent_groups


def equiv_group_mapping(equivalent_groups):
    """
    Create a dictionary mapping each application to its equivalent group number.

    Args:
        equivalent_groups (list of sets): List where each set contains equivalent application IDs.

    Returns:
        dict: Dictionary mapping 'appln_id' to equivalent group number.
    """
    group_map = {}
    for group_num, group in enumerate(equivalent_groups):
        for appln_id in group:
            if appln_id in group_map:
                group_map[appln_id].append(group_num)  # Add group number if multiple
            else:
                group_map[appln_id] = [group_num]  # Assign group number to each application
    return group_map


def get_groups_sharing_patents(group_list):
    """
    Return equivalent groups that share patent applications.

    Args:
        group_list (list): A list of equivalent groups.
    
    Returns:
        list of sets: Each set contains group IDs that share at least one common patent.
    """
    shared_groups = []

    patent_group_mapping = equiv_group_mapping(group_list)

    # Iterate over the patent-group mapping
    for _, group_list in patent_group_mapping.items():
        # If a patent is mapped to more than one group, it indicates shared groups
        if len(group_list) > 1:
            # Convert the list of groups into a set and append it to the shared_groups list
            shared_groups.append(set(group_list))
    
    return shared_groups


def combine_equivalents(equiv_list):
    """
    Filter out cases where a patent is mapped to multiple groups and
    combine those groups and results in disjoint equivalent groups

    Args:
        equiv_list (list): A list of equivalents.

    Returns:
        list of sets: A list where each set contains group IDs that share at least one patent.
    """
    group_count = len(equiv_list)

    # Step 1: Collect groups that share patents
    shared_groups = get_groups_sharing_patents(equiv_list)
    
    # Step 2: Remove duplicate groups by using frozenset to make them hashable
    shared_groups_final = set(frozenset(group_set) for group_set in shared_groups)

    # Step 3: Convert frozensets back to normal sets
    shared_groups_final = [set(group) for group in shared_groups_final]

    # Step 4: Track all group numbers that are part of any merged sets
    shared_group_list_num = set()
    for shared_group_set in shared_groups_final:
        for group_id in shared_group_set:
            shared_group_list_num.add(group_id)

    # Step 5: Identify singleton groups (those that don't share any patents)
    singleton_groups = set(range(group_count)).difference(shared_group_list_num)

    # Step 6: Add singleton groups as individual sets to the final list
    singleton_sets = [equiv_list[group_id] for group_id in singleton_groups]

    # Step 7: Merge groups that share patent
    merged_groups = []
    for shared_group_set in shared_groups_final:
        tmp = set()
        for group_id in shared_group_set:
            tmp.update(equiv_list[group_id])
        merged_groups.append(tmp)

    # Step 8: Combine both shared and singleton groups
    final_groups = singleton_sets + merged_groups

    return final_groups


def save_equivalents(group_list):
    """
    Convert the list of equivalent groups into a DataFrame and save it to a file.

    Args:
        group_list (list of sets): List where each set contains equivalent application IDs.

    Returns:
        None
    """
    # Create a list of rows, each containing the group number and the equivalent application IDs
    data = []
    for group_num, group in enumerate(group_list):
        for appln_id in group:
            data.append({"appln_id": appln_id, "eqv_grp_num": group_num})

    # Convert the data into a pandas DataFrame
    df = pd.DataFrame(data)
    df.sort_values('eqv_grp_num', inplace=True)

    # Save the DataFrame
    df.to_feather(DATA_FOLDER / "patent_equivalents.feather")


if __name__ == '__main__':
    # Load and process priorities
    priority_combined = load_and_process_priorities()

    equiv_0 = priority_combined.groupby('prior_appln_id_str')['appln_id'].agg(list).reset_index()
    equiv_list = create_equivalents(equiv_0)

    equiv_list_curr = equiv_list
    changes = True
    while changes:
        equiv_list_new = combine_equivalents(equiv_list_curr)
        print(len(equiv_list_new))
        if len(equiv_list_curr) == len(equiv_list_new):
            changes = False
        equiv_list_curr = equiv_list_new

    # checking all groups are mutually exclusive
    assert len(get_groups_sharing_patents(equiv_list_curr)) == 0, \
        "Equivalent groups are not mutually exclusive"
    save_equivalents(equiv_list_curr)
