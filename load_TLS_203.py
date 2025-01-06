import pandas as pd
from config import RAW_FOLDER, DATA_FOLDER


TABLE_NAME = "tls203"
files_list = []
for file in RAW_FOLDER.glob(f"{TABLE_NAME}*.csv"):
    _df = pd.read_csv(file, low_memory=False, usecols=['appln_id', 'appln_abstract_lg'])
    print(f"Loaded: {file}")
    files_list.append(_df)
df = pd.concat(files_list, ignore_index=True)
print(df.isna().sum())
df['appln_abstract_lg'] = df['appln_abstract_lg'].str.strip().str.lower()
assert sum(df['appln_abstract_lg'] == '') == 0
del files_list
print(f"shape of {TABLE_NAME}={df.shape}")
#df.to_feather(DATA_FOLDER / f"{TABLE_NAME.upper()}.feather")
df.to_feather(DATA_FOLDER / "appln_abstract_lg.feather")
