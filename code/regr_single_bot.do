clear all
set more off

import delimited "/Users/bobo/Documents/WikipediaAutomation/data/automation_final_table_quality.csv", encoding(ISO-8859-1)

levelsof nwikiproject, local(levels) 

foreach nwp of local levels {
	reg pct_dv_article time_index ls_cv_total_template ls_cv_project_scope if nwikiproject == `nwp'
}

_b[pct_contain_template]
