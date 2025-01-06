
import pandas as pd
from config import DATA_FOLDER


TLS201_FILE = DATA_FOLDER / "TLS201.feather"


appln = pd.read_feather(TLS201_FILE, columns=['appln_id', 'appln_auth',
                                              'appln_filing_date', 'appln_kind',
                                              'appln_nr_epodoc', 'ipr_type',
                                              'docdb_family_id'])

appln[['appln_id', 'docdb_family_id']].to_stata(
    DATA_FOLDER / "appln_docdb_number.dta",
    write_index=False,
    variable_labels={
        'appln_id': 'PATSTAT Appln ID',
        'docdb_family_id': 'DOCDB Family ID'
    }
)

appln.drop('docdb_family_id', axis=1).to_stata(
    DATA_FOLDER / "appln_meta_info_short.dta",
    write_index=False,
    variable_labels={
        'appln_id': 'PATSTAT Appln ID',
        'appln_auth': 'Appln Authority',
        'appln_filing_date': 'Filing Date',
        'appln_kind': 'Appln Kind',
        'appln_nr_epodoc': 'EPODOC number',
        'ipr_type': 'IPR Type'
    }
)
