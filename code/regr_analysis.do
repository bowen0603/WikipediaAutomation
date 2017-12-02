clear all
set more off

import delimited "/Users/bobo/Documents/WikipediaAutomation/data/automation_final_table.csv", encoding(ISO-8859-1)

xtset nwikiproject

*** Descriptions ***
* 1. log transfered and standardized all the IVs
* 2. log transfered and standardized only CV of project previous prod0 and coors45;
*    no action on wp_tenure and l_tcount
* 3. log transfered the two DVs
* how to interpret results?

**** Qs
* 1. operations on quadratic terms?
* 2. How to interpret the values in this case? One std increase of IV, increases the DV by k%?
* 3. How to remove outliers?


***** IVs *****

***** CVs *****
* time index: no change
* previous total edits?

gen l_cv_total_template = ln(cv_total_template+1) / ln(2)
egen ls_cv_total_template = std(l_cv_total_template)

gen l_cv_project_scope = ln(cv_project_scope+1) / ln(2)
egen ls_cv_project_scope = std(l_cv_project_scope)


***** DVs *****


****** Correlation Test ******
corr pct_by_bots pct_contain_template time_index ls_cv_total_template ls_cv_project_scope
corr pct_template_bots pct_template_editors pct_non_template_bots pct_non_template_editors time_index ls_cv_total_template ls_cv_project_scope


***** Model 1 (Baseline) *****
xtreg pct_dv_total time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project time_index ls_cv_total_template ls_cv_project_scope


***** Model 2 % of template and bot edits *****
cor pct_by_bots pct_contain_template time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_total pct_by_bots pct_contain_template time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article pct_by_bots pct_contain_template time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member pct_by_bots pct_contain_template time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project pct_by_bots pct_contain_template time_index ls_cv_total_template ls_cv_project_scope



***** Model 3 template v.s. bots 2X2 *****
xtreg pct_dv_total pct_template_bots pct_template_editors pct_non_template_bots pct_non_template_editors time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article pct_template_bots pct_template_editors pct_non_template_bots pct_non_template_editors time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member pct_template_bots pct_template_editors pct_non_template_bots pct_non_template_editors time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project pct_template_bots pct_template_editors pct_non_template_bots pct_non_template_editors time_index ls_cv_total_template ls_cv_project_scope


***** Model 4 template and coverage *****
corr pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_total pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope

***** Model 5 template v.s. bots v.s. place *****

corr pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_template_article_editor pct_template_member_editor pct_template_project_editor

xtreg pct_dv_total pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_template_article_editor pct_template_member_editor pct_template_project_editor

xtreg pct_dv_article pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_template_article_editor pct_template_member_editor pct_template_project_editor
xtreg pct_dv_member pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_template_article_editor pct_template_member_editor pct_template_project_editor
xtreg pct_dv_project pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_template_article_editor pct_template_member_editor pct_template_project_editor



clear all
set more off

import delimited "/Users/bobo/Documents/WikipediaAutomation/data/stats.csv", encoding(ISO-8859-1)
line pct_by_bots pct_contain_template time
line pct_template_bots pct_template_editors pct_non_template_bots pct_non_template_editors time_index

