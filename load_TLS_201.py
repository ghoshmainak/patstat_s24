"""
load TLS201
"""
import pandas as pd
from config import RAW_FOLDER, DATA_FOLDER


TLS_201_files_list = []
for tls_201_file in RAW_FOLDER.glob("tls201*.csv"):
    _df = pd.read_csv(tls_201_file, low_memory=False)
    print(f"Loaded: {tls_201_file}")
    TLS_201_files_list.append(_df)
TLS_201_df = pd.concat(TLS_201_files_list, ignore_index=True)
del TLS_201_files_list
print(f"shape of TLS_201={TLS_201_df.shape}")
TLS_201_df.appln_kind = TLS_201_df.appln_kind.str.strip()
TLS_201_df.appln_nr_epodoc = TLS_201_df.appln_nr_epodoc.str.strip()
TLS_201_df.appln_nr = TLS_201_df.appln_nr.str.strip()
TLS_201_df.appln_nr_original = TLS_201_df.appln_nr_original.str.strip()
TLS_201_df.appln_auth = TLS_201_df.appln_auth.str.strip()
TLS_201_df = TLS_201_df[TLS_201_df.appln_id != 0].copy()
TLS_201_df = TLS_201_df.replace("9999-12-31", None)
for col in TLS_201_df.columns:
    if "year" in col:
        TLS_201_df[col] = TLS_201_df[col].replace(9999, None)
TLS_201_df['granted_x'] = (TLS_201_df.granted == 'Y').astype(int)
TLS_201_df = TLS_201_df.drop('granted', axis=1).rename(columns={
    'granted_x': 'granted'
})
TLS_201_df.to_feather(DATA_FOLDER / "TLS201.feather")
