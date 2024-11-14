"""
find actual application derived from provisional filing
US
"""
import pandas as pd
from config import DATA_FOLDER


TLS201_FILE = DATA_FOLDER / "TLS201.feather"
PRIORITY_FILE = DATA_FOLDER / "TLS204.feather"
APPLN_PCT_LINK_FILE = DATA_FOLDER / "appln_PCT_link.dta"
DATE_NAN = '9999-12-31'


appln_pct = pd.read_stata(APPLN_PCT_LINK_FILE)
tls201_df = pd.read_feather(TLS201_FILE)
priority = pd.read_feather(PRIORITY_FILE)
obschek = priority.shape[0]
priority = pd.merge(priority,
                    tls201_df[['appln_id', 'appln_auth', 'appln_kind', 'appln_filing_date', 'appln_nr', 'appln_nr_original', 'appln_nr_epodoc']],
                    left_on='prior_appln_id',
                    right_on='appln_id',
                    how='left')
assert obschek == priority.shape[0]
priority = priority.drop(['appln_id_y', 'prior_appln_seq_nr'], axis=1)
priority = priority.rename(columns={
    'appln_id_x': 'appln_id',
    'appln_filing_date': 'prior_filing_date',
    'appln_auth': 'prior_appln_auth',
    'appln_kind': 'prior_appln_kind',
    'appln_nr': 'prior_appln_nr',
    'appln_nr_original': 'prior_appln_nr_original',
    'appln_nr_epodoc': 'prior_appln_nr_epodoc'
})

US_provisional_priority = priority.query("prior_appln_auth == 'US' and prior_appln_kind == 'P'").copy()
US_provisional_priority = US_provisional_priority.sort_values('prior_appln_id')
US_provisional_priority = US_provisional_priority.drop(['prior_appln_auth', 'prior_appln_kind'], axis=1)

obschek = US_provisional_priority.shape[0]
US_provisional_priority = pd.merge(US_provisional_priority,
                                   tls201_df[['appln_id', 'appln_auth', 'appln_kind', 'appln_filing_date', 'appln_nr', 'appln_nr_original', 'appln_nr_epodoc','receiving_office', 'docdb_family_id']],
                                   on='appln_id',
                                   how='left')
assert obschek == US_provisional_priority.shape[0]
US_provisional_priority['US_non_prov_appln'] = (US_provisional_priority.appln_auth == 'US') | (US_provisional_priority.receiving_office == 'US')

# add prior PCT info for actual appln
obschek = US_provisional_priority.shape[0]
US_provisional_priority = pd.merge(US_provisional_priority,
                                   appln_pct,
                                   on='appln_id',
                                   how='left')
assert obschek == US_provisional_priority.shape[0]

US_provisional_priority.prior_filing_date = US_provisional_priority.prior_filing_date.fillna(DATE_NAN)
US_provisional_priority.appln_filing_date = US_provisional_priority.appln_filing_date.fillna(DATE_NAN)

# RULE 1: for each provisional find appln with earliest filing date

US_provisional_priority['rank1'] = US_provisional_priority.groupby('prior_appln_id').appln_filing_date.rank('dense')
US_provisional_priority = US_provisional_priority.query("rank1==1").copy()

# RULE 2: per provisional appln, we prioritize appln
# without prior PCTs
US_provisional_priority['rank2'] = US_provisional_priority.groupby('prior_appln_id').PCT_appln.rank('dense')
US_provisional_priority = US_provisional_priority.query("rank2==1").copy()

# RULE 3: if there is US applnor WO filed in US, take that as a potential one
US_provisional_priority['rank3'] = US_provisional_priority.groupby('prior_appln_id').US_non_prov_appln.rank('dense', ascending=False)
US_provisional_priority = US_provisional_priority.query("rank3==1").copy()

# RULE 4: prioritize as follows, provided belong to same family:
# 1. directly filed at US as in appln_auth == 'US' 
# 2. WO appln
# 3. EP appln
# 4. JP appln
# 5. rest (value 9999)
US_provisional_priority['filing_country_order'] = US_provisional_priority.appln_auth.map(lambda x: 1 if x == 'US' else 2 if x == 'WO' else 3 if x == 'EP' else 4 if x == 'JP' else 9999)
US_provisional_priority['rank4'] = US_provisional_priority.groupby(['prior_appln_id', 'docdb_family_id']).filing_country_order.rank('dense')
US_provisional_priority = US_provisional_priority.query("rank4==1").copy()

# RULE 5: if filed at US, take that
US_provisional_priority['direct_US_filing'] = US_provisional_priority.appln_auth == 'US'
US_provisional_priority['rank5'] = US_provisional_priority.groupby('prior_appln_id').direct_US_filing.rank('dense', ascending=False)
US_provisional_priority = US_provisional_priority.query("rank5==1").copy()

# especially for filing_country_order = 9999
US_provisional_priority['rank6'] = US_provisional_priority.groupby(['prior_appln_id' ,'docdb_family_id']).appln_id.rank('dense')
US_provisional_priority = US_provisional_priority.query("rank6==1").copy()

US_provisional_priority['rank7'] = US_provisional_priority.groupby(['prior_appln_id']).filing_country_order.rank('dense')
US_provisional_priority = US_provisional_priority.query("rank7==1").copy()

US_provisional_priority['rank8'] = US_provisional_priority.groupby(['prior_appln_id']).appln_id.rank('dense')
US_provisional_priority = US_provisional_priority.query("rank8==1").copy()

assert US_provisional_priority.prior_appln_id.duplicated().sum()==0

US_provisional_priority = US_provisional_priority[[
    'prior_appln_id', 'prior_filing_date', 'prior_appln_nr',
    'prior_appln_nr_original', 'prior_appln_nr_epodoc',
    'appln_id', 'appln_auth', 'appln_kind',
    'appln_filing_date', 'appln_nr', 'appln_nr_original', 'appln_nr_epodoc',
]]
US_provisional_priority.columns = [col.replace('prior', 'provisional') for col in US_provisional_priority.columns]

assert US_provisional_priority.provisional_appln_nr_original.notna().sum() == 0
assert US_provisional_priority.provisional_appln_nr_epodoc.notna().sum() == 0
US_provisional_priority = US_provisional_priority.drop(['provisional_appln_nr_original', 'provisional_appln_nr_epodoc'], axis=1)
variable_list = {
    'provisional_appln_id': 'Appln ID - Provisional Filing',
    'provisional_filing_date': 'Provisional Appln Date',
    'provisional_appln_nr': 'Provisional Appln Nr',
    'appln_id': 'Appln ID',
    'appln_auth': 'Appln Auth',
    'appln_kind': 'Appln Kind',
    'appln_filing_date': 'Appln Filing Date',
    'appln_nr': 'Appln Nr',
    'appln_nr_original': 'Orig. Appln Nr',
    'appln_nr_epodoc': 'Appln Nr EPODOC'
}

US_provisional_priority.to_stata(DATA_FOLDER / "US_provisional_to_appln_link.dta",
                                 write_index=False,
                                 variable_labels=variable_list)