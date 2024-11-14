import pandas as pd
from config import RAW_FOLDER, DATA_FOLDER


TLS_211_files_list = []
for tls_211_file in RAW_FOLDER.glob("tls211*.csv"):
    _df = pd.read_csv(tls_211_file, low_memory=False)
    print(f"Loaded: {tls_211_file}")
    TLS_211_files_list.append(_df)
TLS_211_df = pd.concat(TLS_211_files_list, ignore_index=True)
del TLS_211_files_list
print(f"shape of TLS_211={TLS_211_df.shape}")
TLS_211_df = TLS_211_df.replace("9999-12-31", None)
TLS_211_df['publn_first_grant_x'] = (TLS_211_df.publn_first_grant == 'Y').astype(int)
TLS_211_df = TLS_211_df.drop('publn_first_grant', axis=1).rename(columns={
    'publn_first_grant_x': 'publn_first_grant'
})
TLS_211_df = TLS_211_df[TLS_211_df.pat_publn_id != 0].copy()
TLS_211_df.to_feather(DATA_FOLDER / "TLS211.feather")
