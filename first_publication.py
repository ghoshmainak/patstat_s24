"""
1. find first publication per application
"""
import pandas as pd
from config import DATA_FOLDER


TLS201_FILE = DATA_FOLDER / "TLS201.feather"
TLS211_FILE = DATA_FOLDER / "TLS211.feather"


applns = pd.read_feather(TLS201_FILE, columns=['appln_id', 'docdb_family_id','earliest_pat_publn_id'])
all_publn = pd.read_feather(TLS211_FILE,
                            columns=['pat_publn_id',
                                     'publn_auth',
                                     'publn_nr',
                                     'publn_kind',
                                     'publn_date'])

earliest_publication = pd.merge(applns,
                                all_publn,
                                left_on='earliest_pat_publn_id',
                                right_on='pat_publn_id')

earliest_publication = earliest_publication.drop('earliest_pat_publn_id',
                                                 axis=1)
earliest_publication.to_stata(DATA_FOLDER / "first_publn_per_appln.dta",
                              write_index=False,
                              variable_labels={
                                  'appln_id': 'PATSTAT Appln ID',
                                  'docdb_family_id': 'DOCDB Family ID',
                                  'pat_publn_id': 'TLS211 ID',
                                  'publn_auth': 'Publn Auth',
                                  'publn_nr': 'Publn Nr.',
                                  'publn_kind': 'Publn Kind',
                                  'publn_date': 'Publn Date'
                              } )