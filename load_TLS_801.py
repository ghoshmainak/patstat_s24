import pandas as pd
from config import RAW_FOLDER, DATA_FOLDER


TABLE_NAME = "tls801"
files_list = []
for file in RAW_FOLDER.glob(f"{TABLE_NAME}*.csv"):
    _df = pd.read_csv(file, low_memory=False)
    print(f"Loaded: {file}")
    files_list.append(_df)
df = pd.concat(files_list, ignore_index=True)
del files_list
print(f"shape of {TABLE_NAME}={df.shape}")
df.to_stata(DATA_FOLDER / f"{TABLE_NAME.upper()}.dta", write_index=False)