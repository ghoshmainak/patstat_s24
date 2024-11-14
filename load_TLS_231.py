import pandas as pd 
from config import RAW_FOLDER, DATA_FOLDER


TABLE_NAME = "tls231"
columns_needed = ['event_id','appln_id','event_seq_nr', 'event_type',
                  'event_auth', 'event_code','event_filing_date', 'event_publn_date',
                  'event_effective_date', 'event_text']
files_list = []
for file in RAW_FOLDER.glob(f"{TABLE_NAME}*.csv"):
    _df = pd.read_csv(file, usecols=columns_needed, low_memory=False)
    print(f"Loaded: {file}")
    files_list.append(_df)
df = pd.concat(files_list, ignore_index=True)
del files_list
print(f"shape of {TABLE_NAME}={df.shape}")
df.to_feather(DATA_FOLDER / f"{TABLE_NAME.upper()}.feather")
