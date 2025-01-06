cd "E:\PERSONS\Mainak Ghosh\patstat_s24"
do config.do


capture log close
set more off
log using "$LOG_DIR/docdb_npl_citations", t replace
******************************************************

clear
use $DATA_DIR/npl_citations
rename citng_appln_id appln_id
merge m:1 appln_id using $DATA_DIR/appln_docdb_number
keep if _merge == 1 | _merge == 3
drop _merge appln_id is_npl_cited citn_gener_auth
duplicates drop
save $DATA_DIR/docdb_npl_citations

************************************************
log close

