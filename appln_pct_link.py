from pathlib import Path
import pandas as pd
from config import DATA_FOLDER


TLS201_PATH = DATA_FOLDER / "TLS201.feather"
EPWO_LINK_PATH = Path(r"E:\PERSONS\Mainak Ghosh\epwo_linkage\data\EP_WO_link_till_2023.dta")


TLS_201 = pd.read_feather(TLS201_PATH)
EPWO = pd.read_stata(EPWO_LINK_PATH)
appln = TLS_201[['appln_id', 'internat_appln_id']].copy()
appln = pd.merge(appln, EPWO, left_on='appln_id',
                 right_on='appln_id_EP', how='left')
appln = appln.drop('appln_id_EP', axis=1).drop_duplicates()
mask = appln.internat_appln_id > 0 & appln.appln_id_WO.isna()
appln.loc[mask, "appln_id_WO"] = appln.loc[mask, "internat_appln_id"]
appln = appln.rename(columns={
    "appln_id_WO": "WO_appln_id"
}).drop("internat_appln_id", axis=1).drop_duplicates()
appln['PCT_appln'] = appln.WO_appln_id.notna()  # has prior PCT appln
assert appln.shape[0] == TLS_201.shape[0]
assert appln.PCT_appln.sum() == (~appln.WO_appln_id.isna()).sum()
appln = appln[['appln_id', 'WO_appln_id', 'PCT_appln']]
appln.to_stata(DATA_FOLDER / "appln_PCT_link.dta", write_index=False)
