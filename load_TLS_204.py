import pandas as pd
from config import RAW_FOLDER, DATA_FOLDER


TLS_204_files_list = []
for tls_204_file in RAW_FOLDER.glob("tls204*.csv"):
    _df = pd.read_csv(tls_204_file, low_memory=False)
    print(f"Loaded: {tls_204_file}")
    TLS_204_files_list.append(_df)
tls_204_df = pd.concat(TLS_204_files_list, ignore_index=True)
del TLS_204_files_list
print(f"shape of tls_204={tls_204_df.shape}")
tls_204_df.to_feather(DATA_FOLDER / "TLS204.feather")
