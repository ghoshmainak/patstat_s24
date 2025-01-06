import pandas as pd
from config import RAW_FOLDER, DATA_FOLDER


TABLE_NAME = "tls202"
files_list = []
for file in RAW_FOLDER.glob(f"{TABLE_NAME}*.csv"):
    _df = pd.read_csv(file, low_memory=False, usecols=['appln_id', 'appln_title_lg'])
    print(f"Loaded: {file}")
    files_list.append(_df)
df = pd.concat(files_list, ignore_index=True)
print(df.isna().sum())
df['appln_title_lg'] = df['appln_title_lg'].str.strip().str.lower()
assert sum(df['appln_title_lg'] == '') == 0
del files_list
print(f"shape of {TABLE_NAME}={df.shape}")
#df.to_feather(DATA_FOLDER / f"{TABLE_NAME.upper()}.feather")
df.to_feather(DATA_FOLDER / "appln_title_lg.feather")
