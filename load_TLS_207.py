import pandas as pd
from config import RAW_FOLDER, DATA_FOLDER


TLS_207_files_list = []
for tls_207_file in RAW_FOLDER.glob("tls207*.csv"):
    _df = pd.read_csv(tls_207_file, low_memory=False)
    print(f"Loaded: {tls_207_file}")
    TLS_207_files_list.append(_df)
tls_207_df = pd.concat(TLS_207_files_list, ignore_index=True)
del TLS_207_files_list
print(f"shape of tls_207={tls_207_df.shape}")
tls_207_df.to_feather(DATA_FOLDER / "TLS207.feather")

# applicants
applicants = tls_207_df.query("applt_seq_nr > 0").copy()
applicants = applicants.drop("invt_seq_nr", axis=1)
applicants.to_stata(DATA_FOLDER / "applicants_TLS207.dta",
                    write_index=False,
                    variable_labels={
                        'person_id': 'Person ID',
                        'appln_id': 'Appln ID',
                        'applt_seq_nr': 'Applicant seq. nr.'
                    })

# inventors
inventors = tls_207_df.query("invt_seq_nr > 0").copy()
inventors = inventors.drop("applt_seq_nr", axis=1)
inventors.to_stata(DATA_FOLDER / "inventors_TLS207.dta",
                    write_index=False,
                    variable_labels={
                        'person_id': 'Person ID',
                        'appln_id': 'Appln ID',
                        'invt_seq_nr': 'Inventor seq. nr.'
                    })
