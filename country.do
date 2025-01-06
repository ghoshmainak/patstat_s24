cd "E:\PERSONS\Mainak Ghosh\patstat_s24"
do config.do


capture log close
set more off
log using "$LOG_DIR/country", t replace
*******************************************

clear
import delimited using "$COUNTRY_DIR/countries.csv", delimiters(",") varnames(1)
label variable iso2 "2-letter country code"
drop if missing(iso2)

// add PATSTAT TLS801 
d using "$DATA_DIR/TLS801"
rename iso2 ctry_code

preserve
	clear
	use "$DATA_DIR/TLS801"
	
	replace ctry_code = strtrim(ctry_code)
	replace st3_name = strtrim(st3_name)
	replace continent = strtrim(continent)
	replace organisation_flag = strtrim(organisation_flag)
	replace eu_member = strtrim(eu_member)
	replace epo_member = strtrim(epo_member)
	replace oecd_member = strtrim(oecd_member)
	replace discontinued = strtrim(discontinued)
	
	drop if missing(ctry_code)
	label variable st3_name "Short Name"
	label variable discontinued "Entity Non-existent"
	tab continent, missing
	count if continent=="Europe"
	count if continent=="Europe" & organisation_flag=="Y"
	keep ctry_code st3_name continent organisation_flag eu_member epo_member oecd_member discontinued

	tempfile TLS801_countries
	save `TLS801_countries'
restore

merge 1:1 ctry_code using `TLS801_countries'
drop _merge
*** Add two more discontinued countries
*** 1. Southern Rhodesia (RH) 2. Zaire (ZR)
gen is_last_row = (_n == _N)
expand 3 if is_last_row == 1
replace ctry_code = "RH" if _n == _N - 1
replace st3_name = "Southern Rhodesia" if _n == _N - 1

replace ctry_code = "ZR" if _n == _N
replace st3_name = "Zaire" if _n == _N

replace discontinued = "Y" if _n == _N | _n == _N - 1
replace continent = "Africa" if _n == _N | _n == _N - 1
replace country = "" if _n == _N | _n == _N - 1
replace organisation_flag = "" if _n == _N | _n == _N - 1
replace eu_member = "" if _n == _N | _n == _N - 1
replace epo_member = "" if _n == _N | _n == _N - 1
replace oecd_member = "" if _n == _N | _n == _N - 1

drop is_last_row

// add missing continent
preserve
	clear
	import delimited using "$COUNTRY_DIR/country_continent_short.csv", delimiters(",") varnames(1)
	keep iso2 continent
	rename iso2 ctry_code
	rename continent continent_x
	tempfile continent
	save `continent'
restore

merge 1:1 ctry_code using `continent', keep(master matched)
replace continent = continent_x if missing(continent) & !missing(continent_x)
count if missing(continent) & organisation_flag != "Y"
list if missing(continent) & organisation_flag != "Y"
// add continent for Kosovo (XK)
replace continent = "Europe" if ctry_code == "XK"
drop _merge continent_x

save "$DATA_DIR/countries", replace
 
************************************************
log close
