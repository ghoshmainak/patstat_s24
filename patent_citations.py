"""
Find citation for applications
distinguish 
    1. which one is from search authority
    2. which one is from examining division
    3. which one is from applicant
also add citation for PCT, if not added already in TLS212
"""
import os
import pandas as pd
from config import DATA_FOLDER


APPLN_PCT_LINK_PATH = DATA_FOLDER / "appln_PCT_link.dta"
TLS211_FILE = DATA_FOLDER / "TLS211.feather"
TLS212_FILE = DATA_FOLDER / "TLS212.feather"


TLS_211_applications = pd.read_feather(TLS211_FILE)

#################
######## all citaion ###########################
all_appln_citation = pd.read_feather(TLS212_FILE)
all_appln_citation = all_appln_citation.drop_duplicates()
all_appln_citation.cited_npl_publn_id = all_appln_citation.cited_npl_publn_id.str.strip()
all_appln_citation.cited_npl_publn_id = all_appln_citation.cited_npl_publn_id.fillna(
    '0')
assert all_appln_citation.cited_pat_publn_id.isna().sum() == 0
assert all_appln_citation.cited_npl_publn_id.isna().sum() == 0
assert all_appln_citation.cited_appln_id.isna().sum() == 0

all_appln_citation['is_publn_cited'] = all_appln_citation.cited_pat_publn_id > 0
################################################################
# If the CITED_NPL_PUBLN_ID is not 0, and if that NPL citation refers to a patent document,
# then CITED_PAT_PUBLN_ID will hold the value of the PAT_PUBLN_ID of the referenced
# patent document.
print(
    f"""{all_appln_citation.query('cited_pat_publn_id!=0 and cited_npl_publn_id!="0"').shape[0]} cited doc NPL, but cited_pat_publn_id is populated"""
)
assert all_appln_citation.query(
    'cited_appln_id!=0 and cited_npl_publn_id!="0"').shape[0] == 0
all_appln_citation['is_npl_cited'] = (all_appln_citation.cited_npl_publn_id != '0') \
    & (all_appln_citation.is_publn_cited == 0)
all_appln_citation['is_appln_cited'] = (all_appln_citation.is_publn_cited == 0) \
    & (all_appln_citation.is_npl_cited == 0) \
    & (all_appln_citation.cited_appln_id > 0)

print(
    f"Is there any patent that does not cite patent, NPL, and appln = {all_appln_citation.query('is_publn_cited == 0 and is_npl_cited == 0 and is_appln_cited == 0').shape[0]}")
print("Citation distribution")
print(f"publn citation = {all_appln_citation.is_publn_cited.sum()}")
print(f"NPL citation = {all_appln_citation.is_npl_cited.sum()}")
print(f"appln citation = {all_appln_citation.is_appln_cited.sum()}")

##########################################################
# pull citng publn info
obscheck = all_appln_citation.shape[0]  # obs before merge
all_appln_citation_premerge = all_appln_citation.copy()
all_appln_citation = pd.merge(all_appln_citation,
                              TLS_211_applications[['pat_publn_id',
                                                    'publn_auth',
                                                    'appln_id']],
                              on='pat_publn_id')
assert obscheck == all_appln_citation.shape[0]  # same obs after merge
del all_appln_citation_premerge
all_appln_citation = all_appln_citation.rename(columns={
    'pat_publn_id': 'citng_pat_publn_id',
    'publn_auth': 'citng_publn_auth',
    'appln_id': 'citng_appln_id'
})

# citation made to npl
all_appln_citation_npl = all_appln_citation.query("is_npl_cited==True")
#all_appln_citation_npl.drop(columns=['cited_pat_publn_id', 'cited_appln_id', 'pat_citn_seq_nr',
#                                      'is_publn_cited', 'is_npl_cited', 'is_appln_cited']).\
#                                to_feather(DATA_FOLDER / "npl_citations.feather")
# citation made to patent publication
all_appln_citation_pp = all_appln_citation.query("is_publn_cited==True")
# citation made to patent application
all_appln_citation_pa = all_appln_citation.query("is_appln_cited==True")

# publn info for cited patent publication
obscheck = all_appln_citation_pp.shape[0]
all_appln_citation_pp = pd.merge(all_appln_citation_pp,
                                 TLS_211_applications[['pat_publn_id',
                                                       'appln_id']],
                                 left_on='cited_pat_publn_id',
                                 right_on='pat_publn_id')
assert obscheck == all_appln_citation_pp.shape[0]
all_appln_citation_pp = all_appln_citation_pp.drop('pat_publn_id', axis=1)
all_appln_citation_pp.cited_appln_id = all_appln_citation_pp.appln_id
all_appln_citation_pp = all_appln_citation_pp.drop('appln_id', axis=1)

##################################
# combine patent publn, appln and npl cited doc
all_appln_citation_w_publn = pd.concat([all_appln_citation_pp,
                                        all_appln_citation_pa,
                                        all_appln_citation_npl],
                                       ignore_index=True)
assert all_appln_citation.shape[0] == all_appln_citation_w_publn.shape[0]
all_appln_citation_w_publn.to_feather(DATA_FOLDER / "all_citations.feather")
##################################################
# PCT applns go thru search during international phase,
# sometimes citation generated during this phase are missing from applications' citation list
# so, we need to merge back to citation list
# citation for EuroPCT
PCT_phase_citn = all_appln_citation_w_publn.query(
    "citng_publn_auth== 'WO'").copy()
# there should be no citn_replenished for PCT
assert sum(PCT_phase_citn.citn_replenished > 0) == 0

# appln PCT 
appln_pct_link = pd.read_stata(APPLN_PCT_LINK_PATH).query("PCT_appln==1")
appln_pct_link.WO_appln_id = appln_pct_link.WO_appln_id.astype(int)
PCT_phase_citn_origin = pd.merge(appln_pct_link[[
    'appln_id', 'WO_appln_id'
]],
    PCT_phase_citn,
    left_on='WO_appln_id',
    right_on='citng_appln_id')
PCT_phase_citn_origin = PCT_phase_citn_origin.drop('citng_appln_id',
                                                   axis=1).drop_duplicates().copy()

cols_related_citng = [
    col for col in PCT_phase_citn_origin.columns if 'citng' in col]
cols_related_citng.remove('citng_pat_publn_id')
PCT_phase_citn_origin = PCT_phase_citn_origin.drop(
    cols_related_citng, axis=1)

# add citng_pat_publn_id (PCT pat publn) to citn_replenished
PCT_phase_citn_origin['citn_replenished'] = PCT_phase_citn_origin['citng_pat_publn_id']
PCT_phase_citn_origin = PCT_phase_citn_origin.drop('citng_pat_publn_id', axis=1).\
    rename(columns={
        'appln_id': 'citng_appln_id'
    }).drop(['WO_appln_id'], axis=1)


################################################################
# add the above calculated citations made by international offices to regional citations
all_appln_citation = pd.concat([all_appln_citation_w_publn, PCT_phase_citn_origin],
                               ignore_index=True)
all_appln_citation = all_appln_citation.drop(['citng_pat_publn_id', 'citn_replenished', 'citn_id',
                                              'cited_pat_publn_id', 'pat_citn_seq_nr', 'npl_citn_seq_nr',
                                              'citng_publn_auth'], axis=1)
all_pl_citation = all_appln_citation.query("is_publn_cited == 1 or is_appln_cited == 1").copy()
all_pl_citation = all_pl_citation.drop_duplicates(subset=['citng_appln_id',
                                                          'citn_origin',
                                                          'cited_appln_id'])
all_pl_citation = all_pl_citation.drop(['cited_npl_publn_id', 'is_npl_cited'], axis=1)
all_pl_citation = all_pl_citation[['citng_appln_id', 'citn_origin', 'cited_appln_id',
                                   'citn_gener_auth', 'is_publn_cited',
                                   'is_appln_cited']]
all_npl_citation = all_appln_citation.query("is_npl_cited == 1").copy()
all_npl_citation = all_npl_citation.drop_duplicates(subset=['citng_appln_id',
                                                            'citn_origin',
                                                            'cited_npl_publn_id'])
all_npl_citation = all_npl_citation.drop(['cited_appln_id', 'is_publn_cited',
                                          'is_appln_cited'], axis=1)
all_npl_citation = all_npl_citation[['citng_appln_id', 'citn_origin',
                                     'cited_npl_publn_id', 'citn_gener_auth', 'is_npl_cited']]

all_pl_citation.is_publn_cited = all_pl_citation.is_publn_cited.astype(int)
all_pl_citation.is_appln_cited = all_pl_citation.is_appln_cited.astype(int)
all_npl_citation.is_npl_cited = all_npl_citation.is_npl_cited.astype(int)

### save
all_pl_citation.to_stata(DATA_FOLDER / "pl_citations.dta",
                         write_index=False,
                         variable_labels={
                            'cited_appln_id': 'PATSTAT Appln ID (cited doc)',
                            'citng_appln_id': 'PATSTAT Appln ID (citing doc)',
                            'citn_gener_auth': 'Citation generated by',
                            'citn_origin': 'Citation phase',
                            'is_publn_cited': 'Patent publn cited',
                            'is_appln_cited': 'Patent appln cited'
                         })
all_npl_citation.to_stata(DATA_FOLDER / "npl_citations.dta",
                          write_index=False,
                          variable_labels={
                            'cited_npl_publn_id': 'NPL doc ID (cited doc)',
                            'citng_appln_id': 'PATSTAT Appln ID (citing doc)',
                            'citn_gener_auth': 'Citation generated by',
                            'citn_origin': 'Citation phase',
                            'is_npl_cited': 'NPL cited',
                            })

# delete all_citations.feather
os.remove(DATA_FOLDER / "all_citations.feather")