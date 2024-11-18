"""
Generate PCT filings
"""
import pandas as pd
from config import DATA_FOLDER


TLS201_FILE = DATA_FOLDER / "TLS201.feather"
PRIOTITY_FILE = DATA_FOLDER / "earliest_priority.dta"
APPLN_PCT_LINK_FILE = DATA_FOLDER / "appln_PCT_link.dta"


tls201_df = pd.read_feather(TLS201_FILE)
priority_data = pd.read_stata(PRIOTITY_FILE)
PCT_filings = tls201_df.query("appln_auth=='WO'").copy()
obscheck = PCT_filings.shape[0]
PCT_filings = pd.merge(PCT_filings,
                       priority_data,
                       on='appln_id',
                       how='left')
assert obscheck == PCT_filings.shape[0]
PCT_FILE_COLS = ['appln_id', 'appln_auth', 'appln_nr', 'appln_kind',
                 'appln_filing_date', 'appln_nr_epodoc', 'appln_nr_original', 'ipr_type',
                 'receiving_office', 'earliest_prior_appln_id',
                 'earliest_priority_date', 'earliest_prior_appln_auth',
                 'earliest_publn_date', 'earliest_pat_publn_id', 'docdb_family_id']
PCT_filings = PCT_filings[PCT_FILE_COLS].drop_duplicates()

varibale_label ={
    'appln_id': 'Appln ID (PATSTAT)',
    'appln_auth': 'Appln Auth',
    'appln_kind': 'Appln Kind',
    'appln_nr': 'Appln Nr.',
    'appln_nr_epodoc': 'Appln Nr. (EPODOC)',
    'appln_nr_original': 'Orig. Appln Nr.',
    'appln_filing_date': 'Filing Date',
    'ipr_type': 'IPR Type',
    'receiving_office': 'Filed in',
    'earliest_prior_appln_id': 'Priority Appln ID',
    'earliest_priority_date': 'Priority Date',
    'earliest_prior_appln_auth': 'Priority Appln Auth',
    'earliest_publn_date': 'Earliest Publn Date',
    'earliest_pat_publn_id': 'Earliest PAT PUBLN ID (TLS211)',
    'docdb_family_id': 'DOCDB Family ID'
}
PCT_filings.to_stata(DATA_FOLDER / "PCT_filings.dta",
                     write_index=False,
                     variable_labels=varibale_label)

############## PCT to National Filings ##################################################
PCT_filings = PCT_filings[['appln_id', 'appln_nr', 'appln_nr_epodoc', 'appln_nr_original','appln_filing_date', 'docdb_family_id']]
PCT_filings = PCT_filings.rename(columns={
    'appln_id': 'WO_appln_id',
    'appln_nr': 'WO_appln_nr',
    'appln_nr_epodoc': 'WO_appln_nr_epodoc',
    'appln_nr_original': 'WO_appln_nr_original',
    'appln_filing_date': 'WO_filing_date'
})
PCT_2_NAT_filings = pd.merge(PCT_filings,
                             tls201_df[['appln_id', 'appln_auth', 'appln_kind', 'appln_nr', 'appln_nr_epodoc', 'appln_nr_original', 'appln_filing_date', 'docdb_family_id']],
                             on='docdb_family_id')
assert PCT_2_NAT_filings.WO_appln_id.unique().shape[0] == PCT_filings.WO_appln_id.unique().shape[0]
PCT_2_NAT_filings = PCT_2_NAT_filings.query("WO_appln_id!=appln_id").copy()

varibale_label_2 = {
    'WO_appln_id': 'PCT Appln ID',
    'WO_appln_nr': 'PCT Appln Nr.',
    'WO_appln_nr_epodoc': 'PCT Appln Nr. (EPODOC)',
    'WO_appln_nr_original': 'Orig. PCT Appln Nr.',
    'WO_appln_filing_date': 'PCT Filing Date',
    'docdb_family_id': 'DOCDB Family ID',
    'appln_id': 'Appln ID (Family Member)',
    'appln_auth': 'Appln Auth (Family Member)',
    'appln_kind': 'Appln Kind (Family Member)',
    'appln_nr': 'Appln Nr. (Family Member)',
    'appln_nr_epodoc': 'Appln Nr. EPODOC (Family)',
    'appln_nr_original': 'Orig. Appln Nr. (Family)',
    'appln_filing_date': 'Filing Date (Family Member)'
}
PCT_2_NAT_filings.to_stata(DATA_FOLDER / "PCT_national_filings.dta",
                           write_index=False,
                           variable_labels=varibale_label_2)

############### PCT families ################################################
appln_PCT_link = pd.read_stata(APPLN_PCT_LINK_FILE)
appln_PCT_link = appln_PCT_link.query("PCT_appln==1").copy()
appln_PCT_link.drop('PCT_appln', axis=1, inplace=True)
appln_PCT_link['WO_appln_id'] = appln_PCT_link['WO_appln_id'].astype(int)
list_2_add_wo_appln = []
for wo_apln_id in appln_PCT_link['WO_appln_id'].unique():
    list_2_add_wo_appln.append((wo_apln_id, wo_apln_id))
df_2_add_appln = pd.DataFrame(list_2_add_wo_appln, columns=appln_PCT_link.columns)
appln_PCT_link = pd.concat([appln_PCT_link, df_2_add_appln], ignore_index=True)
appln_PCT_link.sort_values('WO_appln_id', inplace=True)
appln_PCT_link.reset_index(drop=True, inplace=True)
appln_PCT_link.to_stata(DATA_FOLDER / "PCT_families.dta",
                        write_index=False)
