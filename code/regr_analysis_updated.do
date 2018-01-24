clear all
set more off

*import delimited "/Users/bobo/Documents/WikipediaAutomation/data/automation_final_table_updated.csv", encoding(ISO-8859-1)
*import delimited "/Users/bobo/Documents/WikipediaAutomation/data/automation_final_table.csv", encoding(ISO-8859-1)
import delimited "/Users/bobo/Documents/WikipediaAutomation/data/automation_final_table_quality.csv", encoding(ISO-8859-1)

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

gen l_cv_total_by_bots = ln(by_bots+1) / ln(2)
egen ls_cv_total_by_bots = std(l_cv_total_by_bots)

gen l_cv_total_template = ln(cv_total_template+1) / ln(2)
egen ls_cv_total_template = std(l_cv_total_template)

gen l_cv_project_scope = ln(cv_project_scope+1) / ln(2)
egen ls_cv_project_scope = std(l_cv_project_scope)

gen l_cv_pre_edits = ln(cv_pre_edits+1) / ln(2)
egen ls_cv_pre_edits = std(l_cv_pre_edits)

gen l_cv_pre_article = ln(cv_pre_article+1) / ln(2)
egen ls_cv_pre_article = std(l_cv_pre_article)

gen l_cv_pre_member = ln(cv_pre_member+1) / ln(2)
egen ls_cv_pre_member = std(l_cv_pre_member)

gen l_cv_pre_project = ln(cv_pre_project+1) / ln(2)
egen ls_cv_pre_project = std(l_cv_pre_project)

gen l_cv_active_members = ln(cv_active_members+1) / ln(2)
egen ls_cv_active_members = std(l_cv_active_members)

gen l_cv_article_bot = ln(article_bot+1) / ln(2)
egen ls_cv_article_bot = std(l_cv_article_bot)

gen l_cv_member_bot = ln(member_bot+1) / ln(2)
egen ls_cv_member_bot = std(l_cv_member_bot)

gen l_cv_project_bot = ln(project_bot+1) / ln(2)
egen ls_cv_project_bot = std(l_cv_project_bot)

gen l_cv_article_template = ln(template_article+1) / ln(2)
egen ls_cv_article_template = std(l_cv_article_template)

gen l_cv_member_template = ln(template_member+1) / ln(2)
egen ls_cv_member_template = std(l_cv_member_template)

gen l_cv_project_template = ln(template_project+1) / ln(2)
egen ls_cv_project_template = std(l_cv_project_template)

***** DVs *****


****** Correlation Test ******
corr pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
regress pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
vif

corr pct_template_bots pct_template_editors pct_non_template_bots pct_non_template_editors time_index ls_cv_total_template ls_cv_project_scope

cv_wp_tenure cv_pre_edits cv_active_members 

***** Model 1 (Baseline) *****
xtreg delta_quality time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project time_index ls_cv_total_template ls_cv_project_scope

xtreg delta_quality time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_article time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_member time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 


***** Model 2 Effects of bot editing *****
cor pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg delta_quality pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 

xtreg pct_dv_article pct_by_bots time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member pct_by_bots time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project pct_by_bots time_index ls_cv_total_template ls_cv_project_scope

***** Model 3 Effects of template usage *****
cor pct_by_bots pct_contain_template time_index ls_cv_total_template ls_cv_project_scope
xtreg delta_quality pct_contain_template time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article pct_contain_template time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member pct_contain_template time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project pct_contain_template time_index ls_cv_total_template ls_cv_project_scope

*** combining model 2 and 3 together
xtreg delta_quality pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_article pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_member pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_project pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 

* update quality DV
gen l_delta_quality = ln(delta_quality) / ln(2)

xtreg delta_quality pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg l_delta_quality pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_article pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_member pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_project pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 

***** Model 4 Effects of template used by bots *****
xtreg delta_quality pct_by_bots pct_contain_template pct_template_bots time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article pct_by_bots pct_contain_template pct_template_bots time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member pct_by_bots pct_contain_template pct_template_bots time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project pct_by_bots pct_contain_template pct_template_bots time_index ls_cv_total_template ls_cv_project_scope

* updated
xtreg delta_quality pct_template_bots pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_article pct_template_bots pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_member pct_template_bots pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_project pct_template_bots pct_by_bots pct_contain_template time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 

***** Model 5 bots v.s. place *****
corr pct_article_bot pct_member_bot pct_project_bot time_index ls_cv_total_template ls_cv_project_scope

xtreg delta_quality pct_article_bot pct_member_bot pct_project_bot time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article pct_article_bot pct_member_bot pct_project_bot time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member pct_article_bot pct_member_bot pct_project_bot time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project pct_article_bot pct_member_bot pct_project_bot time_index ls_cv_total_template ls_cv_project_scope

* updated
regress pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
vif

xtreg delta_quality pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_article pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_member pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_project pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 

* correcting CVs
regress pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits ls_cv_article_bot ls_cv_member_bot ls_cv_project_bot
vif

xtreg delta_quality pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits ls_cv_article_bot ls_cv_member_bot ls_cv_project_bot
xtreg pct_dv_article pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits ls_cv_article_bot ls_cv_member_bot ls_cv_project_bot 
xtreg pct_dv_member pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits ls_cv_article_bot ls_cv_member_bot ls_cv_project_bot
xtreg pct_dv_project pct_article_bot pct_member_bot pct_project_bot time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits ls_cv_article_bot ls_cv_member_bot ls_cv_project_bot 



***** Model 6 template v.s. place *****
corr pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope

xtreg delta_quality pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project pct_template_article pct_template_member pct_template_project time_index ls_cv_total_template ls_cv_project_scope


* updated
xtreg delta_quality pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_article pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_member pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_project pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 


* correcting CVs
xtreg delta_quality pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits ls_cv_article_template ls_cv_member_template ls_cv_project_template
xtreg pct_dv_article pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits ls_cv_article_template ls_cv_member_template ls_cv_project_template 
xtreg pct_dv_member pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits ls_cv_article_template ls_cv_member_template ls_cv_project_template
xtreg pct_dv_project pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits ls_cv_article_template ls_cv_member_template ls_cv_project_template 


***** Model 6 template v.s. bots v.s. place *****

corr pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_template_article_editor pct_template_member_editor pct_template_project_editor

xtreg delta_quality pct_template_article_bot pct_template_member_bot pct_template_project_bot time_index ls_cv_total_template ls_cv_project_scope

xtreg pct_dv_article pct_template_article_bot pct_template_member_bot pct_template_project_bot time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_member pct_template_article_bot pct_template_member_bot pct_template_project_bot time_index ls_cv_total_template ls_cv_project_scope
xtreg pct_dv_project pct_template_article_bot pct_template_member_bot pct_template_project_bot time_index ls_cv_total_template ls_cv_project_scope

* updated
xtreg delta_quality pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_article_bot pct_member_bot pct_project_bot pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_article pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_article_bot pct_member_bot pct_project_bot pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_member pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_article_bot pct_member_bot pct_project_bot pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 
xtreg pct_dv_project pct_template_article_bot pct_template_member_bot pct_template_project_bot pct_article_bot pct_member_bot pct_project_bot pct_template_article pct_template_member pct_template_project time_index cv_wp_tenure ls_cv_project_scope ls_cv_active_members ls_cv_pre_edits 



// clear all
// set more off
//
// import delimited "/Users/bobo/Documents/WikipediaAutomation/data/stats.csv", encoding(ISO-8859-1)
// line pct_by_bots pct_contain_template time
// line pct_template_bots pct_template_editors pct_non_template_bots pct_non_template_editors time_index

