############################ TLS 205 #################################

# A technical family is created manually on request when documents disclosing identical
# subject matter (i.e. having identical description and drawings) are not automatically
# grouped together because they do not claim the same priority or combination of priorities.
# The reasons why applicants may decide not to claim a priority are of various kinds: in
# some cases, the 12-month period foreseen in the Paris Convention might have been
# exceeded; in other cases, there might be economic reasons (e.g. innovation subsidies
# based on patent filings); yet in other cases, it could be related to the different ways in
# which IP offices - based on their respective IP laws - deal with patent continuations,
# divisionals and additions.
# Also known as Intellectual priorities

######################################################################

import pandas as pd
from config import RAW_FOLDER, DATA_FOLDER


TABLE_NAME = "tls205"
files_list = []
for file in RAW_FOLDER.glob(f"{TABLE_NAME}*.csv"):
    _df = pd.read_csv(file, low_memory=False)
    print(f"Loaded: {file}")
    files_list.append(_df)
df = pd.concat(files_list, ignore_index=True)
del files_list
print(f"shape of {TABLE_NAME}={df.shape}")
df.to_feather(DATA_FOLDER / f"{TABLE_NAME.upper()}.feather")
