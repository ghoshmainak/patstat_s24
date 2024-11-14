"""
create a separate file for appln docdb pair
"""
import pandas as pd
from config import DATA_FOLDER


TLS201_FILE = DATA_FOLDER / "TLS201.feather"


appln = pd.read_feather(TLS201_FILE, columns=['appln_id', 'docdb_family_id'])
appln.to_stata(DATA_FOLDER / "appln_docdb_number.dta",
               write_index=False,
               variable_labels={
                   'appln_id': 'PATSTAT Appln ID',
                   'docdb_family_id': 'DOCDB Family ID'
               })
