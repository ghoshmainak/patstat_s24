"""
Generate names for application-applicant pairs
"""
import pandas as pd
from config import DATA_FOLDER


APPLICANT_FILE = DATA_FOLDER / "applicants_TLS207.dta"
NAME_FILE = DATA_FOLDER / "person_names.feather"
TLS201_FILE = DATA_FOLDER / "TLS201.feather"


### load
TLS201 = pd.read_feather(TLS201_FILE, columns=['appln_id'])
applicants = pd.read_stata(APPLICANT_FILE)
names = pd.read_feather(NAME_FILE)

applicant_names = pd.merge(applicants,
                           names,
                           on='person_id',
                           how='left')
assert applicant_names.person_id.unique().shape[0] == applicants.person_id.unique().shape[0]

applicant_names = pd.merge(TLS201[['appln_id']],
                           applicant_names,
                           on='appln_id',
                           how='left')
assert applicant_names.appln_id.unique().shape[0] == TLS201.appln_id.unique().shape[0]

applicant_names_exist = applicant_names.query("applt_seq_nr.notna()").copy()
applicant_names_exist.person_id = applicant_names_exist.person_id.astype(int)
applicant_names_exist.applt_seq_nr = applicant_names_exist.applt_seq_nr.astype(int)

applicant_names_exist.to_feather(DATA_FOLDER / "applicantnames.feather")

# first applicant
first_applicant = applicant_names_exist.query("applt_seq_nr==1").copy()
first_applicant = first_applicant.drop("applt_seq_nr", axis=1)
first_applicant.to_feather(DATA_FOLDER / "1stapplicantnames.feather")

######################### IMPUTATION for missing applicants ###################


