"""
create quasi-priorities for applications
that will consist of:
1. priority
2. prior-PCT
3. tech-rel
4. continuation
"""
############# EP quasi priorities with priority order ################
# technically similar application, though, represents identical subject matter,
# due to non explicit declaration for priority, we consider technically similar ones
# as priority like, but not the priority
# continuation are not always same invention, so put them at the lowest in the order
# priority order:
# priorities > PCT > Technically similar > Continuation
##########################################################################

import pandas as pd
from config import DATA_FOLDER


APPLN_PCT_LINK_PATH = DATA_FOLDER / "appln_PCT_link.dta"
DATE_NAN = "9999-12-31"


# TLS201
TLS201 = pd.read_feather(DATA_FOLDER / "TLS201.feather")

# priority
priority = pd.read_feather(DATA_FOLDER / "TLS204.feather")
priority = priority.drop('prior_appln_seq_nr', axis=1)
obscheck = priority.shape[0]
priority = pd.merge(priority,
                    TLS201[['appln_id', 'appln_auth', 'appln_filing_date']].\
                        rename(
                          columns={"appln_id": "prior_appln_id",
                                   "appln_auth": "prior_appln_auth",
                                   "appln_filing_date": "prior_appln_filing_date"
                                   }
                                ),
                    on='prior_appln_id')
assert obscheck == priority.shape[0]
priority['source_priority'] = 1

# PCT
PCT = pd.read_stata(APPLN_PCT_LINK_PATH).query("PCT_appln==1")
obscheck = PCT.shape[0]
PCT = pd.merge(PCT,
                    TLS201[['appln_id', 'appln_auth', 'appln_filing_date']].\
                        rename(
                          columns={"appln_id": "WO_appln_id",
                                   "appln_auth": "prior_appln_auth",
                                   "appln_filing_date": "prior_appln_filing_date"
                                   }
                                ),
                    on='WO_appln_id')
assert obscheck == PCT.shape[0]
PCT = PCT.rename(columns={'WO_appln_id': 'prior_appln_id'})
PCT['source_PCT'] = 1

# TLS205
TLS205 = pd.read_feather(DATA_FOLDER / "TLS205.feather")
obscheck = TLS205.shape[0]
TLS205 = pd.merge(TLS205,
                  TLS201[['appln_id', 'appln_auth', 'appln_filing_date']],
                  on='appln_id')
assert obscheck == TLS205.shape[0]
obscheck = TLS205.shape[0]
TLS205 = pd.merge(TLS205,
                  TLS201[['appln_id', 'appln_auth', 'appln_filing_date']].\
                    rename(columns={
                        "appln_id": "tech_rel_appln_id",
                        "appln_auth": "tech_rel_appln_auth",
                        "appln_filing_date": "tech_rel_appln_filing_date"
                        }
                        ),
                on='tech_rel_appln_id')
assert obscheck == TLS205.shape[0]
TLS205['priority_like'] = TLS205.appln_filing_date >= TLS205.tech_rel_appln_filing_date
TLS205_priority_like = TLS205.query("priority_like==True").\
    drop(["priority_like", 'appln_auth', 'appln_filing_date'], axis=1)
TLS205_priority_like.columns = TLS205_priority_like.columns.map(
    lambda x: x.replace('tech_rel', 'prior'))
TLS205_priority_like['source_tech_rel'] = 1

# TLS216
TLS216 = pd.read_feather(DATA_FOLDER / "TLS216.feather")
obscheck = TLS216.shape[0]
TLS216 = pd.merge(TLS216,
                    TLS201[['appln_id', 'appln_auth', 'appln_filing_date']].\
                        rename(
                          columns={"appln_id": "parent_appln_id",
                                   "appln_auth": "prior_appln_auth",
                                   "appln_filing_date": "prior_appln_filing_date"
                                   }
                                ),
                    on='parent_appln_id')
assert obscheck == TLS216.shape[0]
TLS216 = TLS216.rename(columns={'parent_appln_id': 'prior_appln_id'})
TLS216['source_continuation'] = 1

# quasi priorities
quasi_priorities = pd.merge(priority,
                            PCT,
                            how='outer',
                            on=['appln_id', 'prior_appln_id', 'prior_appln_auth', 'prior_appln_filing_date']).drop_duplicates()

quasi_priorities = pd.merge(quasi_priorities,
                            TLS205_priority_like,
                            how='outer',
                            on=['appln_id', 'prior_appln_id', 'prior_appln_auth', 'prior_appln_filing_date']).drop_duplicates()

quasi_priorities = pd.merge(quasi_priorities,
                            TLS216,
                            how='outer',
                            on=['appln_id', 'prior_appln_id', 'prior_appln_auth', 'prior_appln_filing_date']).drop_duplicates()

quasi_priorities.source_priority = quasi_priorities.source_priority.fillna(
    0).astype(int)
quasi_priorities.source_PCT = quasi_priorities.source_PCT.fillna(
    0).astype(int)
quasi_priorities.source_tech_rel = quasi_priorities.source_tech_rel.fillna(
    0).astype(int)
quasi_priorities.source_continuation = quasi_priorities.source_continuation.fillna(
    0).astype(int)

# ############### quasi priority order:
# # priorities > PCT > Technically similar > Continuation
source_value_label = {
    'source': {
        1: 'priority',
        2: 'prior-PCT',
        3: 'tech-rel',
        4: 'continuation'
    }
}
quasi_priorities_order = quasi_priorities.query("source_priority==1").copy()
quasi_priorities_order['source'] = 1
quasi_priorities_order = pd.concat([quasi_priorities_order,
                                    quasi_priorities.query("source_priority==0 and source_PCT==1")],
                                    ignore_index=True)
quasi_priorities_order['source'] = quasi_priorities_order['source'].fillna(2)
quasi_priorities_order = pd.concat([quasi_priorities_order,
                                    quasi_priorities.query("source_priority==0 and source_PCT==0 and source_tech_rel==1")],
                                    ignore_index=True)
quasi_priorities_order['source'] = quasi_priorities_order['source'].fillna(3)
quasi_priorities_order = pd.concat([quasi_priorities_order,
                                    quasi_priorities.query("source_priority==0 and source_PCT==0 and source_tech_rel==0 and source_continuation==1")],
                                    ignore_index=True)
quasi_priorities_order['source'] = quasi_priorities_order['source'].fillna(4)
quasi_priorities_order = quasi_priorities_order.drop(
    ['source_priority', 'source_PCT', 'source_tech_rel',
     'source_continuation', 'PCT_appln', 'contn_type'], axis=1)
quasi_priorities_order = quasi_priorities_order.sort_values(['appln_id',
                                                             'source',
                                                             'prior_appln_filing_date',
                                                             'prior_appln_id']).reset_index(drop=True).drop_duplicates()

quasi_priorities_order.prior_appln_id = quasi_priorities_order.prior_appln_id.astype(int)
quasi_priorities_order.source = quasi_priorities_order.source.astype(int)
quasi_priorities_order.to_stata(DATA_FOLDER / "applns_quasi_priorities.dta",
                                write_index=False,
                                variable_labels={
                                    'appln_id': 'Appln ID (PATSTAT)',
                                    'prior_appln_id': 'Priority Appln ID (PATSTAT)',
                                    'prior_appln_filing_date': 'Priority Filing Date',
                                    'prior_appln_auth': 'Priority Appln Auth',
                                    'source': 'Priority Link'
                                },
                                value_labels=source_value_label
                                )




