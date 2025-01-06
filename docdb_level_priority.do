cd "E:\PERSONS\Mainak Ghosh\patstat_s24"
do config.do


capture log close
set more off
log using "$LOG_DIR/docdb_level_priority", t replace
******************************************************

clear
use $DATA_DIR/appln_docdb_number

// add priority
merge 1:1 appln_id using $DATA_DIR/earliest_priority, keepusing(earliest_priority_date) keep(master matched)
drop _merge appln_id
// checking if there is any unknown priority date
count if earliest_priority_date == "$DATE_NAN"
replace earliest_priority_date = "" if earliest_priority_date == "$DATE_NAN"
gen earliest_priority_date_x = date(earliest_priority_date, "YMD")
drop earliest_priority_date
rename earliest_priority_date_x earliest_priority_date
format earliest_priority_date %td

collapse (min) family_earliest_priority_date = earliest_priority_date, by(docdb_family_id)
label variable family_earliest_priority_date "Earliest priority date in DOCDB"
gen family_earliest_priority_year = year(family_earliest_priority_date)
label variable family_earliest_priority_year "Earliest priority year in DOCDB"
duplicates report
mdesc

save $DATA_DIR/DOCDB_earliest_priority, replace

************************************************
log close
