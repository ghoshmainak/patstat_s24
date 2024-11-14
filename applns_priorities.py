"""
Link priorities for all applications
find oldest priority too
"""
import pandas as pd
from config import DATA_FOLDER


TLS201_PATH = DATA_FOLDER / "TLS201.feather"
TLS204_PATH = DATA_FOLDER / "TLS204.feather"
DATE_NAN = "9999-12-31"


TLS_201 = pd.read_feather(TLS201_PATH)
TLS_204 = pd.read_feather(TLS204_PATH)

############ First Filing ########################
appln = pd.merge(TLS_201[['appln_id']], TLS_204[['appln_id', 'prior_appln_id']],
                 on='appln_id', how='left')
appln['first_filing'] = appln.prior_appln_id.isna().astype(int)
appln = appln.drop('prior_appln_id', axis=1).drop_duplicates()
appln.to_stata(DATA_FOLDER / "first_filings.dta",
               write_index=False,
               variable_labels={
                        'appln_id': 'PATSTAT Appln ID',
                        'first_filing': 'First Filing'
                    })
################################################################

###################### all priorities from TLS 204 #########################
all_priorities = pd.merge(TLS_201['appln_id'],
                          TLS_204,
                          on='appln_id')

all_priorities = pd.merge(all_priorities,
                          TLS_201[['appln_id', 'appln_filing_date', 'appln_auth']],
                          left_on='prior_appln_id',
                          right_on='appln_id')
all_priorities = all_priorities.rename(columns={
    'appln_filing_date': 'priority_date',
    'appln_id_x': 'appln_id',
    'appln_auth': 'prior_appln_auth'
})
all_priorities = all_priorities.drop(['appln_id_y','prior_appln_seq_nr'], axis=1)
all_priorities = all_priorities.sort_values('appln_id')
all_priorities.priority_date = all_priorities.priority_date.fillna(DATE_NAN)

# earliest priority from TLS 204
all_priorities = all_priorities.sort_values(['appln_id', 'priority_date', 'prior_appln_id'])
all_priorities['earliest_priority'] = all_priorities.groupby('appln_id').\
    priority_date.rank(method='first')
earliest_priority = all_priorities.loc[all_priorities.earliest_priority==1].copy()


#############################################################

# if no priority, then appln itself is priority
obscheck = appln.shape[0]
appln = pd.merge(appln,
                 TLS_201[['appln_id', 'appln_filing_date', 'appln_auth']],
                 on='appln_id')
assert obscheck == appln.shape[0]
appln.appln_filing_date = appln.appln_filing_date.fillna(DATE_NAN)
first_filing = appln[appln.first_filing == 1].copy()
first_filing['prior_appln_id'] = first_filing.appln_id
first_filing['priority_date'] = first_filing.appln_filing_date
first_filing['prior_appln_auth'] = first_filing.appln_auth

# final dataset
earliest_priority = pd.concat([first_filing, earliest_priority],
                              ignore_index=True)
assert obscheck == earliest_priority.shape[0]
earliest_priority = earliest_priority.drop(['appln_filing_date', 'appln_auth'],
                                           axis=1)
earliest_priority = earliest_priority.rename(columns={'prior_appln_id': 'earliest_prior_appln_id',
                                                     'prior_appln_auth': 'earliest_prior_appln_auth',
                                                     'priority_date': 'earliest_priority_date'})
earliest_priority = earliest_priority.drop(['first_filing', 'earliest_priority'],
                                           axis=1)
earliest_priority = earliest_priority.sort_values('appln_id')
earliest_priority.to_stata(DATA_FOLDER / "earliest_priority.dta",
                           write_index=False,
                           variable_labels={
                               'appln_id': 'Appln ID (PATSTAT)',
                               'earliest_prior_appln_id': 'Earliest Priority Appln ID (PATSTAT)',
                               'earliest_priority_date': 'Earliest Priority Filing Date',
                               'earliest_prior_appln_auth': 'Earliest Priority Appln Auth'
                           })
