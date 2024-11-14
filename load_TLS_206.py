import pandas as pd
from config import RAW_FOLDER, DATA_FOLDER


TLS_206_files_list = []
for tls_206_file in RAW_FOLDER.glob("tls206*.csv"):
    _df = pd.read_csv(tls_206_file, low_memory=False)
    print(f"Loaded: {tls_206_file}")
    TLS_206_files_list.append(_df)
tls_206_df = pd.concat(TLS_206_files_list, ignore_index=True)
del TLS_206_files_list
print(f"shape of tls_206={tls_206_df.shape}")
tls_206_df.doc_std_name = tls_206_df.doc_std_name.str.strip()
tls_206_df.han_name = tls_206_df.han_name.str.strip()
tls_206_df.psn_name = tls_206_df.psn_name.str.strip()
tls_206_df.person_ctry_code = tls_206_df.person_ctry_code.str.strip()
tls_206_df.psn_sector = tls_206_df.psn_sector.str.strip()
tls_206_df.person_address = tls_206_df.person_address.str.strip()

tls_206_df.to_feather(DATA_FOLDER / "TLS206.feather")

# names
names = tls_206_df[['person_id', 'person_name', 'doc_std_name', 'psn_name', 'han_name']].drop_duplicates()
names.to_feather(DATA_FOLDER / "person_names.feather")

# country code
tls_206_df[['person_id', 'person_ctry_code']].\
    drop_duplicates().to_stata(DATA_FOLDER / "person_ctry_code.dta",
                               write_index=False,
                               variable_labels={
                                   'person_id': 'Person ID',
                                   'person_ctry_code': 'Person Country'
                               })

# sector
sector_value = {
    'INDIVIDUAL': 1,
    'UNKNOWN': 2,
    'COMPANY': 3,
    'GOV NON-PROFIT': 4,
    'UNIVERSITY': 5,
    'COMPANY GOV NON-PROFIT': 6,
    'HOSPITAL': 7,
    'GOV NON-PROFIT UNIVERSITY': 8,
    'COMPANY HOSPITAL': 9,
    'COMPANY UNIVERSITY': 10,
    'COMPANY GOV NON-PROFIT UNIVERSITY': 11,
    'GOV NON-PROFIT HOSPITAL': 12,
    'COMPANY INDIVIDUAL': 13,
}
sector_value_reverse = {}
for k,v in sector_value.items():
    sector_value_reverse[v] = k

tls_206_df.psn_sector = tls_206_df.psn_sector.map(lambda x: sector_value[x] if x is not None else None)
tls_206_df[['person_id', 'psn_sector']].dropna(subset=['psn_sector']).\
    drop_duplicates().to_stata(DATA_FOLDER / "person_sector.dta",
                               write_index=False,
                               variable_labels={
                                   'person_id': 'Person ID',
                                   'psn_sector': 'Sector'
                               },
                               value_labels={
                                   'psn_sector': sector_value_reverse
                               })

# address
tls_206_df[['person_id', 'person_address']].dropna(subset=['person_address']).\
    drop_duplicates().to_feather(DATA_FOLDER / "person_address.feather")
