__author__ = 'bobo'
from GQuerier import QueryHandler


class Executor:

    # need to run manually: create_edits_on_project_pages()

    def __init__(self):
        self.query = QueryHandler()
        self.default_db = "bowen_wikipedia_raw" #"bowen_automation"
        self.raw_revs = "revs_2017"
        self.time_table = "time_index_3month"

        # variables to change
        self.article_projects_table = "article_projects"
        self.valid_projects = "valid_wikiprojects"

    def main(self):

        # self.select_valid_projects()

        # # generate longitudinal data (IV) for three namespaces
        # self.create_edits_on_project_article_pages()
        # self.create_edits_on_project_pages()
        # self.create_edits_on_project_member_pages()
        #
        # # compute independent variables
        # self.create_longitudinal_data()
        # self.create_variable_types_by_longitudinal_data()
        #
        # # # compute quality as DVs
        # self.project_quality_change()
        #
        # self.compute_CVs()
        #
        # # # merge all the variables
        self.merging_tables()
        #
        # # # finalize the table
        self.compute_variables()

        # self.generate_data_for_qualitative_article_quality()

        # self.generate_data_for_qualitative_article_productivity()
        #
        # self.generate_data_for_qualitative_project_coordination()
        #
        # self.generate_data_for_qualitative_member_communication()

        # self.create_user_pools()

    def create_user_pools(self):

        # v1: active in the project within 30 days
        # v2: active in the project within 90 days
        # v3: active in the project within most recent 500 edits
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.total_candidates AS new_total_v1,
                (t1.total_candidates - t1.overlapped_candidates) AS new_unique_v1,
                t2.total_candidates AS new_total_v2,
                (t2.total_candidates - t2.overlapped_candidates) AS new_unique_v2
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject
        """.format(self.default_db, "newcomers1",
                   self.default_db, "newcomers2")
        self.query.run_query(query, self.default_db, "newcomers_exp1")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.new_total_v1 AS new_total_v1,
                t1.new_total_v2 AS new_total_v2,
                t2.total_candidates AS new_total_v3,
                t1.new_unique_v1 AS new_unique_v1,
                t1.new_unique_v2 AS new_unique_v2,
                (t2.total_candidates - t2.overlapped_candidates) AS new_unique_v3
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject
        """.format(self.default_db, "newcomers_exp1",
                   self.default_db, "newcomers3")
        self.query.run_query(query, self.default_db, "newcomers_exp2")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.new_total_v1 AS new_total_v1,
                t1.new_total_v2 AS new_total_v2,
                t1.new_total_v3 AS new_total_v3,
                t1.new_unique_v1 AS new_unique_v1,
                t1.new_unique_v2 AS new_unique_v2,
                t1.new_unique_v3 AS new_unique_v3,
                t2.total_candidates AS exp_total_v1,
                (t2.total_candidates - t2.overlapped_candidates) AS exp_unique_v1
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject
        """.format(self.default_db, "newcomers_exp2",
                   self.default_db, "experienced_editors1")
        self.query.run_query(query, self.default_db, "newcomers_exp3")


        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.new_total_v1 AS new_total_v1,
                t1.new_total_v2 AS new_total_v2,
                t1.new_total_v3 AS new_total_v3,
                t1.new_unique_v1 AS new_unique_v1,
                t1.new_unique_v2 AS new_unique_v2,
                t1.new_unique_v3 AS new_unique_v3,
                t1.exp_total_v1 AS exp_total_v1,
                t2.total_candidates AS exp_total_v2,
                t1.exp_unique_v1 AS exp_unique_v1,
                (t2.total_candidates - t2.overlapped_candidates) AS exp_unique_v2
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject
        """.format(self.default_db, "newcomers_exp3",
                   self.default_db, "experienced_editors2")
        self.query.run_query(query, self.default_db, "newcomers_exp4")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.new_total_v1 AS new_total_v1,
                t1.new_total_v2 AS new_total_v2,
                t1.new_total_v3 AS new_total_v3,
                t1.new_unique_v1 AS new_unique_v1,
                t1.new_unique_v2 AS new_unique_v2,
                t1.new_unique_v3 AS new_unique_v3,
                t1.exp_total_v1 AS exp_total_v1,
                t1.exp_total_v2 AS exp_total_v2,
                t2.total_candidates AS exp_total_v3,
                t1.exp_unique_v1 AS exp_unique_v1,
                t1.exp_unique_v2 AS exp_unique_v2,
                (t2.total_candidates - t2.overlapped_candidates) AS exp_unique_v3
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject
                ORDER BY t1.new_total_v1 DESC
        """.format(self.default_db, "newcomers_exp4",
                   self.default_db, "experienced_editors3")
        self.query.run_query(query, self.default_db, "newcomers_exp5")

        query = """
            SELECT ranking,
                LOWER(REPLACE(REPLACE(wikiproject, "_", ""), "WikiProject", "")) AS wikiproject,
                non_bot_edits
                FROM `{}.{}`
        """.format(self.default_db, "active_wikiprojects_by45")
        self.query.run_query(query, self.default_db, "active_wikiprojects")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t2.ranking AS ranking,
                t1.new_total_v1 AS new_total_v1,
                t1.new_total_v2 AS new_total_v2,
                t1.new_total_v3 AS new_total_v3,
                t1.new_unique_v1 AS new_unique_v1,
                t1.new_unique_v2 AS new_unique_v2,
                t1.new_unique_v3 AS new_unique_v3,
                t1.exp_total_v1 AS exp_total_v1,
                t1.exp_total_v2 AS exp_total_v2,
                t1.exp_total_v3 AS exp_total_v3,
                t1.exp_unique_v1 AS exp_unique_v1,
                t1.exp_unique_v2 AS exp_unique_v2,
                t1.exp_unique_v3 AS exp_unique_v3
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject
                ORDER BY t2.ranking ASC
        """.format(self.default_db, "newcomers_exp5",
                   self.default_db, "active_wikiprojects")
        self.query.run_query(query, self.default_db, "newcomers_exp")

    ## 1. bot's edits on project pages that decrease project page activities
    ## 2. bot's edits on member pages that increase project page activities
    def generate_data_for_qualitative_project_coordination(self):
        # analysis one: identify the bots that always appear in the time periods project page activities decrease
        # top 20% periods that have the most decrease in article quality
        query = """
            SELECT wikiproject,
                time_index,
                pct_dv_project
                FROM `{}.{}`
                ORDER BY pct_dv_project ASC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor1")

        # check which are the projects that have many project project coordination increase
        query = """
            SELECT wikiproject,
                COUNT(*) AS cnt
                FROM `{}.{}`
                GROUP BY wikiproject
                ORDER BY cnt DESC
        """.format(self.default_db, "qualitative_analysis_project_coor1")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor2")

        # These are edits made on project pages (type = 3)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_project AS pct_dv_project,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 3 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_project_coor1",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor3")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_project) AS pct_dv_project
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_project_coor3")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor4")

        query = """
            SELECT user_text,
                AVG(pct_dv_project) AS pct_dv_project,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_project_coor4")
        self.query.run_query(query, self.default_db, "bots_caused_project_coor_decrease45")

        # top 20% periods that have the most increase in article productivity
        query = """
            SELECT wikiproject,
                time_index,
                pct_dv_project
                FROM `{}.{}`
                ORDER BY pct_dv_project DESC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor5")

        # These are edits made on project pages (type = 3)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_project AS pct_dv_project,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 3 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_project_coor5",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor6")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_project) AS pct_dv_project
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_project_coor6")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor7")

        query = """
            SELECT user_text,
                AVG(pct_dv_project) AS pct_dv_project,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_project_coor7")
        self.query.run_query(query, self.default_db, "bots_caused_project_coor_increase45")

        # removing overlaps of the two sets of bots
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_project AS pct_dv_project,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_project_coor_decrease45",
                   self.default_db, "bots_caused_project_coor_increase45")
        self.query.run_query(query, self.default_db, "bots_decrease_art_prod45")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_project AS pct_dv_project,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_project_coor_increase45",
                   self.default_db, "bots_caused_project_coor_decrease45")
        self.query.run_query(query, self.default_db, "bots_increase_project_coor45")

        # check the ratio of the two sets
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_project AS de_project_coor,
                t1.appearance AS de_appearance,
                t2.pct_dv_project AS in_project_coor,
                t2.appearance AS in_appearance,
                (t1.appearance / t2.appearance) AS de_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY de_ratio DESC
        """.format(self.default_db, "bots_caused_project_coor_decrease45",
                   self.default_db, "bots_caused_project_coor_increase45")
        self.query.run_query(query, self.default_db, "bots_decrease_ratio_project_coor45")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_project AS de_project_coor,
                t1.appearance AS in_appearance,
                t2.pct_dv_project AS in_project_coor,
                t2.appearance AS de_appearance,
                (t1.appearance / t2.appearance) AS in_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY in_ratio DESC
        """.format(self.default_db, "bots_caused_project_coor_increase45",
                   self.default_db, "bots_caused_project_coor_decrease45")
        self.query.run_query(query, self.default_db, "bots_increase_ratio_project_coor45")

        # These are edits made on member pages (type = 2)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_project AS pct_dv_project,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 2 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_project_coor1",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor3")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_project) AS pct_dv_project
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_project_coor3")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor4")

        query = """
            SELECT user_text,
                AVG(pct_dv_project) AS pct_dv_project,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_project_coor4")
        self.query.run_query(query, self.default_db, "bots_caused_project_coor_decrease23")

        # top 20% periods that have the most increase in article productivity
        query = """
            SELECT wikiproject,
                time_index,
                pct_dv_project
                FROM `{}.{}`
                ORDER BY pct_dv_project DESC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor5")

        # These are edits made on member pages (type = 2)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_project AS pct_dv_project,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 2 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_project_coor5",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor6")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_project) AS pct_dv_project
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_project_coor6")
        self.query.run_query(query, self.default_db, "qualitative_analysis_project_coor7")

        query = """
            SELECT user_text,
                AVG(pct_dv_project) AS pct_dv_project,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_project_coor7")
        self.query.run_query(query, self.default_db, "bots_caused_project_coor_increase23")

        # removing overlaps of the two sets of bots
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_project AS pct_dv_project,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_project_coor_decrease23",
                   self.default_db, "bots_caused_project_coor_increase23")
        self.query.run_query(query, self.default_db, "bots_decrease_project_coor23")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_project AS pct_dv_project,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_project_coor_increase23",
                   self.default_db, "bots_caused_project_coor_decrease23")
        self.query.run_query(query, self.default_db, "bots_increase_project_coor23")

        # check the ratio of the two sets
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_project AS de_project_coor,
                t1.appearance AS de_appearance,
                t2.pct_dv_project AS in_project_coor,
                t2.appearance AS in_appearance,
                (t1.appearance / t2.appearance) AS de_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY de_ratio DESC
        """.format(self.default_db, "bots_caused_project_coor_decrease23",
                   self.default_db, "bots_caused_project_coor_increase23")
        self.query.run_query(query, self.default_db, "bots_decrease_ratio_project_coor23")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_project AS de_project_coor,
                t1.appearance AS in_appearance,
                t2.pct_dv_project AS in_project_coor,
                t2.appearance AS de_appearance,
                (t1.appearance / t2.appearance) AS in_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY in_ratio DESC
        """.format(self.default_db, "bots_caused_project_coor_increase23",
                   self.default_db, "bots_caused_project_coor_decrease23")
        self.query.run_query(query, self.default_db, "bots_increase_ratio_project_coor23")


    # 1. Bot's edits on project pages that decrease member communication
    def generate_data_for_qualitative_member_communication(self):
        # analysis one: identify the bots that always appear in the time periods
        # top 20% periods that have the most decrease in member communication
        query = """
            SELECT wikiproject,
                time_index,
                pct_dv_member
                FROM `{}.{}`
                ORDER BY pct_dv_member ASC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis_member_comm1")

        # check which are the projects that have many member communication increase
        query = """
            SELECT wikiproject,
                COUNT(*) AS cnt
                FROM `{}.{}`
                GROUP BY wikiproject
                ORDER BY cnt DESC
        """.format(self.default_db, "qualitative_analysis_member_comm1")
        self.query.run_query(query, self.default_db, "qualitative_analysis_member_comm2")

        # These are edits made on project pages (type = 3)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_member AS pct_dv_member,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 3 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_member_comm1",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_member_comm3")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_member) AS pct_dv_member
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_member_comm3")
        self.query.run_query(query, self.default_db, "qualitative_analysis_member_comm4")

        query = """
            SELECT user_text,
                AVG(pct_dv_member) AS pct_dv_member,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_member_comm4")
        self.query.run_query(query, self.default_db, "bots_caused_member_comm_decrease45")

        # top 20% periods that have the most increase in member communication
        query = """
            SELECT wikiproject,
                time_index,
                pct_dv_member
                FROM `{}.{}`
                ORDER BY pct_dv_member DESC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis_member_comm5")

        # These are edits made on project pages (type = 3)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_member AS pct_dv_member,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 3 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_member_comm5",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_member_comm6")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_member) AS pct_dv_member
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_member_comm6")
        self.query.run_query(query, self.default_db, "qualitative_analysis_member_comm7")

        query = """
            SELECT user_text,
                AVG(pct_dv_member) AS pct_dv_member,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_member_comm7")
        self.query.run_query(query, self.default_db, "bots_caused_member_comm_increase45")

        # removing overlaps of the two sets of bots
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_member AS pct_dv_member,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_member_comm_decrease45",
                   self.default_db, "bots_caused_member_comm_increase45")
        self.query.run_query(query, self.default_db, "bots_decrease_member_comm45")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_member AS pct_dv_member,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_member_comm_increase45",
                   self.default_db, "bots_caused_member_comm_decrease45")
        self.query.run_query(query, self.default_db, "bots_increase_member_comm45")

        # check the ratio of the two sets
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_member AS de_member_comm,
                t1.appearance AS de_appearance,
                t2.pct_dv_member AS in_member_comm,
                t2.appearance AS in_appearance,
                (t1.appearance / t2.appearance) AS de_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY de_ratio DESC
        """.format(self.default_db, "bots_caused_member_comm_decrease45",
                   self.default_db, "bots_caused_member_comm_increase45")
        self.query.run_query(query, self.default_db, "bots_decrease_ratio_member_comm45")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_member AS de_member_comm,
                t1.appearance AS in_appearance,
                t2.pct_dv_member AS in_member_comm,
                t2.appearance AS de_appearance,
                (t1.appearance / t2.appearance) AS in_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY in_ratio DESC
        """.format(self.default_db, "bots_caused_member_comm_increase45",
                   self.default_db, "bots_caused_member_comm_decrease45")
        self.query.run_query(query, self.default_db, "bots_increase_ratio_member_comm45")

    # create tables of bots to answer two questions:
    # 1. why bot edits on article pages increase article productivity
    # 2. why bot edits on project pages increase article productivity
    def generate_data_for_qualitative_article_productivity(self):
        # analysis one: identify the bots that always appear in the time periods project article productivity decreased
        # top 20% periods that have the most decrease in article quality
        query = """
            SELECT wikiproject,
                time_index,
                pct_dv_article
                FROM `{}.{}`
                ORDER BY pct_dv_article ASC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod1")

        # check which are the projects that have many project article productivity decrease
        query = """
            SELECT wikiproject,
                COUNT(*) AS cnt
                FROM `{}.{}`
                GROUP BY wikiproject
                ORDER BY cnt DESC
        """.format(self.default_db, "qualitative_analysis_art_prod1")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod2")

        # These are edits made on article pages (type = 1)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_article AS pct_dv_article,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 1 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_art_prod1",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod3")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_article) AS pct_dv_article
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_art_prod3")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod4")

        query = """
            SELECT user_text,
                AVG(pct_dv_article) AS pct_dv_article,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_art_prod4")
        self.query.run_query(query, self.default_db, "bots_caused_art_prod_decrease01")

        # top 20% periods that have the most increase in article productivity
        query = """
            SELECT wikiproject,
                time_index,
                pct_dv_article
                FROM `{}.{}`
                ORDER BY pct_dv_article DESC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod5")

        # These are edits made on article pages (type = 1)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_article AS pct_dv_article,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 1 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_art_prod5",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod6")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_article) AS pct_dv_article
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_art_prod6")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod7")

        query = """
            SELECT user_text,
                AVG(pct_dv_article) AS pct_dv_article,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_art_prod7")
        self.query.run_query(query, self.default_db, "bots_caused_art_prod_increase01")

        # removing overlaps of the two sets of bots
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_article AS pct_dv_article,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_art_prod_decrease01",
                   self.default_db, "bots_caused_art_prod_increase01")
        self.query.run_query(query, self.default_db, "bots_decrease_art_prod01")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_article AS pct_dv_article,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_art_prod_increase01",
                   self.default_db, "bots_caused_art_prod_decrease01")
        self.query.run_query(query, self.default_db, "bots_increase_art_prod01")

        # check the ratio of the two sets
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_article AS de_art_prod,
                t1.appearance AS de_appearance,
                t2.pct_dv_article AS in_art_prod,
                t2.appearance AS in_appearance,
                (t1.appearance / t2.appearance) AS de_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY de_ratio DESC
        """.format(self.default_db, "bots_caused_art_prod_decrease01",
                   self.default_db, "bots_caused_art_prod_increase01")
        self.query.run_query(query, self.default_db, "bots_decrease_ratio_art_prod01")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_article AS de_art_prod,
                t1.appearance AS in_appearance,
                t2.pct_dv_article AS in_art_prod,
                t2.appearance AS de_appearance,
                (t1.appearance / t2.appearance) AS in_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY in_ratio DESC
        """.format(self.default_db, "bots_caused_art_prod_increase01",
                   self.default_db, "bots_caused_art_prod_decrease01")
        self.query.run_query(query, self.default_db, "bots_increase_ratio_art_prod01")


        ## Work on namespace 45 - how project page edits increases article productivity
        # These are edits made on article pages (type = 3)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_article AS pct_dv_article,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 3 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_art_prod1",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod3")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_article) AS pct_dv_article
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_art_prod3")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod4")

        query = """
            SELECT user_text,
                AVG(pct_dv_article) AS pct_dv_article,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_art_prod4")
        self.query.run_query(query, self.default_db, "bots_caused_art_prod_decrease45")

        # top 20% periods that have the most increase in article productivity
        query = """
            SELECT wikiproject,
                time_index,
                pct_dv_article
                FROM `{}.{}`
                ORDER BY pct_dv_article DESC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod5")

        # These are edits made on article pages (type = 3)
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.pct_dv_article AS pct_dv_article,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 3 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis_art_prod5",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod6")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(pct_dv_article) AS pct_dv_article
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis_art_prod6")
        self.query.run_query(query, self.default_db, "qualitative_analysis_art_prod7")

        query = """
            SELECT user_text,
                AVG(pct_dv_article) AS pct_dv_article,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis_art_prod7")
        self.query.run_query(query, self.default_db, "bots_caused_art_prod_increase45")

        # removing overlaps of the two sets of bots
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_article AS pct_dv_article,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_art_prod_decrease45",
                   self.default_db, "bots_caused_art_prod_increase45")
        self.query.run_query(query, self.default_db, "bots_decrease_art_prod45")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_article AS pct_dv_article,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_art_prod_increase45",
                   self.default_db, "bots_caused_art_prod_decrease45")
        self.query.run_query(query, self.default_db, "bots_increase_art_prod45")

        # check the ratio of the two sets
        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_article AS de_art_prod,
                t1.appearance AS de_appearance,
                t2.pct_dv_article AS in_art_prod,
                t2.appearance AS in_appearance,
                (t1.appearance / t2.appearance) AS de_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY de_ratio DESC
        """.format(self.default_db, "bots_caused_art_prod_decrease45",
                   self.default_db, "bots_caused_art_prod_increase45")
        self.query.run_query(query, self.default_db, "bots_decrease_ratio_art_prod45")

        query = """
            SELECT t1.user_text AS bot,
                t1.pct_dv_article AS de_art_prod,
                t1.appearance AS in_appearance,
                t2.pct_dv_article AS in_art_prod,
                t2.appearance AS de_appearance,
                (t1.appearance / t2.appearance) AS in_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY in_ratio DESC
        """.format(self.default_db, "bots_caused_art_prod_increase45",
                   self.default_db, "bots_caused_art_prod_decrease45")
        self.query.run_query(query, self.default_db, "bots_increase_ratio_art_prod45")


    def generate_data_for_qualitative_article_quality(self):

        # analysis one: identify the bots that always appear in the time periods project article quality decreased
        # top 20% periods that have the most decrease in article quality
        query = """
            SELECT wikiproject,
                time_index,
                delta_quality
                FROM `{}.{}`
                ORDER BY delta_quality ASC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis1")

        # check which are the projects that have many project quality decrease
        query = """
            SELECT wikiproject,
                COUNT(*) AS cnt
                FROM `{}.{}`
                GROUP BY wikiproject
                ORDER BY cnt DESC
        """.format(self.default_db, "qualitative_analysis1")
        self.query.run_query(query, self.default_db, "qualitative_analysis2")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.delta_quality AS delta_quality,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 1 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis1",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis3")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(delta_quality) AS delta_quality
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis3")
        self.query.run_query(query, self.default_db, "qualitative_analysis4")

        query = """
            SELECT user_text,
                AVG(delta_quality) AS delta_quality,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis4")
        self.query.run_query(query, self.default_db, "bots_caused_quality_decrease")

        # top 20% periods that have the most decrease in article quality
        query = """
            SELECT wikiproject,
                time_index,
                delta_quality
                FROM `{}.{}`
                ORDER BY delta_quality DESC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis5")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.delta_quality AS delta_quality,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 1 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis5",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis6")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(delta_quality) AS delta_quality
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis6")
        self.query.run_query(query, self.default_db, "qualitative_analysis7")

        query = """
            SELECT user_text,
                AVG(delta_quality) AS delta_quality,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis7")
        self.query.run_query(query, self.default_db, "bots_caused_quality_increase")

        # removing overlaps of the two sets of bots
        query = """
            SELECT t1.user_text AS bot,
                t1.delta_quality AS delta_quality,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_quality_decrease",
                   self.default_db, "bots_caused_quality_increase")
        self.query.run_query(query, self.default_db, "bots_decrease_quality")

        query = """
            SELECT t1.user_text AS bot,
                t1.delta_quality AS delta_quality,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_quality_increase",
                   self.default_db, "bots_caused_quality_decrease")
        self.query.run_query(query, self.default_db, "bots_increase_quality")

        # check the ratio of the two sets
        query = """
            SELECT t1.user_text AS bot,
                t1.delta_quality AS de_delta_quality,
                t1.appearance AS de_appearance,
                t2.delta_quality AS in_delta_quality,
                t2.appearance AS in_appearance,
                (t1.appearance / t2.appearance) AS de_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10 AND t1.delta_quality < 0
                 ORDER BY de_ratio DESC
        """.format(self.default_db, "bots_caused_quality_decrease",
                   self.default_db, "bots_caused_quality_increase")
        self.query.run_query(query, self.default_db, "bots_quality_decrease_ratio01")

        query = """
            SELECT t1.user_text AS bot,
                t1.delta_quality AS de_delta_quality,
                t1.appearance AS in_appearance,
                t2.delta_quality AS in_delta_quality,
                t2.appearance AS de_appearance,
                (t1.appearance / t2.appearance) AS in_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY in_ratio DESC
        """.format(self.default_db, "bots_caused_quality_increase",
                   self.default_db, "bots_caused_quality_decrease")
        self.query.run_query(query, self.default_db, "bots_quality_increase_ratio01")

        ## continue to compute the active period and total edits by the bots from those two sets
        query = """
            SELECT *
                FROM `{}.{}`
                LIMIT 100
        """.format(self.default_db, "bots_increase_ratio")
        self.query.run_query(query, self.default_db, "top_good_bots")

        query = """
            SELECT t1.bot AS bot,
                t2.rev_timestamp AS timestamp,
                t2.ns AS ns,
                t2.add_template AS add_template
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.bot = t2.rev_user_text
        """.format(self.default_db, "top_good_bots",
                   self.default_db, self.raw_revs)
        self.query.run_query(query, self.default_db, "good_bots_edits")

        query = """
            SELECT bot,
                (MAX(timestamp) - MIN(timestamp)) / (3600*24*30) AS tenure,
                COUNT(*) AS total_edits
                FROM `{}.{}`
                GROUP BY bot
        """.format(self.default_db, "good_bots_edits")
        self.query.run_query(query, self.default_db, "good_bots")
        # avg tenure: 35.97
        # avg edits: 55618.12

        query = """
            SELECT *
                FROM `{}.{}`
                LIMIT 100
        """.format(self.default_db, "bots_decrease_ratio")
        self.query.run_query(query, self.default_db, "top_bad_bots")

        query = """
            SELECT t1.bot AS bot,
                t2.rev_timestamp AS timestamp,
                t2.ns AS ns,
                t2.add_template AS add_template
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.bot = t2.rev_user_text
        """.format(self.default_db, "top_bad_bots",
                   self.default_db, self.raw_revs)
        self.query.run_query(query, self.default_db, "bad_bots_edits")

        query = """
            SELECT bot,
                (MAX(timestamp) - MIN(timestamp)) / (3600*24*30) AS tenure,
                COUNT(*) AS total_edits
                FROM `{}.{}`
                GROUP BY bot
        """.format(self.default_db, "bad_bots_edits")
        self.query.run_query(query, self.default_db, "bad_bots")
        # avg tenure: 30.4
        # avg edits: 34320.45

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.delta_quality AS delta_quality,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 2 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis1",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis3")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(delta_quality) AS delta_quality
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis3")
        self.query.run_query(query, self.default_db, "qualitative_analysis4")

        query = """
            SELECT user_text,
                AVG(delta_quality) AS delta_quality,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis4")
        self.query.run_query(query, self.default_db, "bots_caused_quality_decrease")

        # top 20% periods that have the most decrease in article quality
        query = """
            SELECT wikiproject,
                time_index,
                delta_quality
                FROM `{}.{}`
                ORDER BY delta_quality DESC
                LIMIT 7200
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "qualitative_analysis5")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.delta_quality AS delta_quality,
                t2.user_text AS user_text,
                t2.ns AS ns,
                t2.add_template AS add_template,
                t2.contain_template AS contain_template
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index AND t2.type = 2 AND t2.is_bot = 1
        """.format(self.default_db, "qualitative_analysis5",
                   self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "qualitative_analysis6")

        # TODO: set threshold for the count
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_edits,
                AVG(delta_quality) AS delta_quality
                FROM `{}.{}`
                GROUP BY wikiproject, time_index, user_text
                HAVING bot_edits >= 2
                ORDER BY wikiproject, time_index
        """.format(self.default_db, "qualitative_analysis6")
        self.query.run_query(query, self.default_db, "qualitative_analysis7")

        query = """
            SELECT user_text,
                AVG(delta_quality) AS delta_quality,
                COUNT(*) AS appearance
                FROM `{}.{}`
                GROUP BY user_text
                ORDER BY appearance DESC
        """.format(self.default_db, "qualitative_analysis7")
        self.query.run_query(query, self.default_db, "bots_caused_quality_increase")

        # removing overlaps of the two sets of bots
        query = """
            SELECT t1.user_text AS bot,
                t1.delta_quality AS delta_quality,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_quality_decrease",
                   self.default_db, "bots_caused_quality_increase")
        self.query.run_query(query, self.default_db, "bots_decrease_quality")

        query = """
            SELECT t1.user_text AS bot,
                t1.delta_quality AS delta_quality,
                t1.appearance AS appearance
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.user_text = t2.user_text
                WHERE t2.user_text IS NULL
                ORDER BY appearance DESC
        """.format(self.default_db, "bots_caused_quality_increase",
                   self.default_db, "bots_caused_quality_decrease")
        self.query.run_query(query, self.default_db, "bots_increase_quality")

        # check the ratio of the two sets
        query = """
            SELECT t1.user_text AS bot,
                t1.delta_quality AS de_delta_quality,
                t1.appearance AS de_appearance,
                t2.delta_quality AS in_delta_quality,
                t2.appearance AS in_appearance,
                (t1.appearance / t2.appearance) AS de_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY de_ratio DESC
        """.format(self.default_db, "bots_caused_quality_decrease",
                   self.default_db, "bots_caused_quality_increase")
        self.query.run_query(query, self.default_db, "bots_quality_decrease_ratio23")

        query = """
            SELECT t1.user_text AS bot,
                t1.delta_quality AS de_delta_quality,
                t1.appearance AS in_appearance,
                t2.delta_quality AS in_delta_quality,
                t2.appearance AS de_appearance,
                (t1.appearance / t2.appearance) AS in_ratio
                 FROM `{}.{}` AS t1
                 INNER JOIN `{}.{}` AS t2
                 ON t1.user_text = t2.user_text
                 WHERE (t1.appearance + t2.appearance) > 10
                 ORDER BY in_ratio DESC
        """.format(self.default_db, "bots_caused_quality_increase",
                   self.default_db, "bots_caused_quality_decrease")
        self.query.run_query(query, self.default_db, "bots_quality_increase_ratio23")

    def compute_CVs(self):
        # project tenure
        query = """
            SELECT wikiproject_page AS wikiproject,
                MIN(first_edit) AS creation_ts
                FROM `{}.{}`
                GROUP BY wikiproject
        """.format(self.default_db, "project_pages_45")
        self.query.run_query(query, self.default_db, "cv_project_tenure1")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t2.index AS index,
                (t2.starting_time - t1.creation_ts) AS wp_tenure
                FROM `{}.{}` t1
                CROSS JOIN `{}.{}` t2
                WHERE t2.starting_time > t1.creation_ts
        """.format(self.default_db, "cv_project_tenure1",
                   self.default_db, self.time_table)
        self.query.run_query(query, self.default_db, "cv_project_tenure2")

        # todo: check here, tenure in months, not the time unit
        query = """
            SELECT wikiproject,
                index,
                wp_tenure / (3600*24*30) AS wp_tenure
                FROM `{}.{}`
        """.format(self.default_db, "cv_project_tenure2")
        self.query.run_query(query, self.default_db, "cv_project_tenure")

        # active users
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS active_members
                FROM `{}.{}`
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "user_active_period45")
        self.query.run_query(query, self.default_db, "cv_active_members")

    def project_quality_change(self):

        # article quality at the beginning of each time period
        query = """
            SELECT LOWER(t1.title) AS article,
                    CASE WHEN t1.prediction = "Start" THEN 1
                    WHEN t1.prediction = "Stub" THEN 2
                    WHEN t1.prediction = "C" THEN 3
                    WHEN t1.prediction = "B" THEN 4
                    WHEN t1.prediction = "A" THEN 5
                    WHEN t1.prediction = "GA" THEN 6
                    WHEN t1.prediction = "FA" THEN 7
                    ELSE 0
                    END AS quality,
                    t2.starting_time AS starting_time,
                    t2.index AS index
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.timestamp = t2.ending_date
        """.format(self.default_db, "article_quality_2016",
                   self.default_db, self.time_table)
        self.query.run_query(query, self.default_db, "article_quality1")

        # query = """
        #     SELECT COUNT(DISTINCT(title)) AS total_article
        #     FROM `{}.{}`
        # """.format(self.default_db, "article_quality_2016")
        # self.query.run_query(query, self.default_db, "article_quality_total")

        query = """
            SELECT wikiproject,
                COUNT(*) AS total_article
                FROM `{}.{}`
                GROUP BY wikiproject
        """.format(self.default_db, "article_projects_2017")
        self.query.run_query(query, self.default_db, "article_quality_total")

        query = """
            SELECT t1.article AS article,
                t1.quality AS quality,
                t2.wikiproject AS wikiproject,
                t1.starting_time AS starting_time,
                t1.index AS index,
                (t1.index+1) AS next_index
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.article = t2.title
        """.format(self.default_db, "article_quality1",
                   self.default_db, "article_projects_2017")
        self.query.run_query(query, self.default_db, "article_quality2")

        # query = """
        #     SELECT t1.article AS article,
        #         t1.quality AS quality,
        #         t1.wikiproject AS wikiproject,
        #         t1.starting_time AS starting_time,
        #         t1.index AS index,
        #         t1.next_index AS next_index,
        #         t2.total_article AS total_article
        #         FROM `{}.{}` AS t1
        #         CROSS JOIN `{}.{}` AS t2
        # """.format(self.default_db, "article_quality2",
        #            self.default_db, "article_quality_total")
        # self.query.run_query(query, self.default_db, "article_quality3")

        query = """
            SELECT t1.article AS article,
                t1.quality AS quality,
                t1.wikiproject AS wikiproject,
                t1.starting_time AS starting_time,
                t1.index AS index,
                t1.next_index AS next_index,
                t2.total_article AS total_article
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject
        """.format(self.default_db, "article_quality2",
                   self.default_db, "article_quality_total")
        self.query.run_query(query, self.default_db, "article_quality3")

        query = """
            SELECT wikiproject,
                index,
                SUM(quality) AS total_quality,
                AVG(total_article) AS total_article
                FROM `{}.{}`
                GROUP BY wikiproject, index
        """.format(self.default_db, "article_quality3")
        self.query.run_query(query, self.default_db, "article_quality4")

        query = """
            SELECT wikiproject,
                index,
                (total_quality / total_article) AS avg_quality
                FROM `{}.{}`
        """.format(self.default_db, "article_quality4")
        self.query.run_query(query, self.default_db, "article_quality5")

        query = """
            SELECT wikiproject,
                avg_quality AS avg_quality,
                index,
                (index+1) AS next_index
                FROM `{}.{}`
        """.format(self.default_db, "article_quality5")
        self.query.run_query(query, self.default_db, "article_quality6")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.avg_quality AS quality,
                t2.avg_quality AS next_quality,
                t1.index AS index
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.next_index = t2.index AND t1.wikiproject = t2.wikiproject
        """.format(self.default_db, "article_quality6",
                   self.default_db, "article_quality6")
        self.query.run_query(query, self.default_db, "article_quality7")

        query = """
            SELECT wikiproject,
                index,
                quality,
                next_quality,
                (next_quality - quality) AS delta_quality
                FROM `{}.{}`
                ORDER BY wikiproject, index
        """.format(self.default_db, "article_quality7")
        self.query.run_query(query, self.default_db, "article_quality_final")


    def generate_longitudinal_IVDVs(self):

        # number of active bots in each time period
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS bot_cnt
            FROM `{}.{}`
            WHERE is_bot = 1
            GROUP BY wikiproject, time_index, user_text
        """.format(self.default_db, "lng_rev_ns01")
        self.query.run_query(query, self.default_db, "lng_article_edits_bot1")

        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS bot_cnt
            FROM `{}.{}`
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_article_edits_bot1")
        self.query.run_query(query, self.default_db, "lng_article_edits_bot")


        # number of active editors in each time period
        query = """
            SELECT wikiproject,
                time_index,
                user_text,
                COUNT(*) AS editor_cnt
            FROM `{}.{}`
            WHERE is_bot = 0
            GROUP BY wikiproject, time_index, user_text
        """.format(self.default_db, "lng_rev_ns01")
        self.query.run_query(query, self.default_db, "lng_article_edits_editor1")

        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS editor_cnt
            FROM `{}.{}`
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_article_edits_editor1")
        self.query.run_query(query, self.default_db, "lng_article_edits_editor")

        # union the two tables
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                IFNULL(t1.bot_cnt, 0) AS bot_cnt,
                IFNULL(t2.editor_cnt, 0) AS editor_cnt
            FROM `{}.{}` AS t1
            FULL OUTER JOIN `{}.{}` AS t2
            ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "lng_article_edits_bot",
                   self.default_db, "lng_article_edits_editor")
        self.query.run_query(query, self.default_db, "lng_rev_ns01_bots_editors")



        # todo: add up the active editors and bots across ns01, 45, 23
        # todo: this needs to be redone as same bots/editors might edit different namespaces



        # total bot

        # total edits for DV
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS total_edits45
            FROM `{}.{}`
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns45")
        self.query.run_query(query, self.default_db, "dv_rev_ns45")

        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS total_edits01
            FROM `{}.{}`
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns01")
        self.query.run_query(query, self.default_db, "dv_rev_ns01")


    def create_longitudinal_data(self):

        ### Longitudinal data for article page edits (ns 01)
        query = """
            SELECT t1.user_text AS user_text,
                t1.title AS title,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t1.contain_template AS contain_template,
                t2.index AS time_index
            FROM `{}.{}` AS t1
            CROSS JOIN `{}.{}` AS t2
            WHERE t1.timestamp < t2.ending_time AND t1.timestamp >= t2.starting_time
        """.format(self.default_db, "rev_ns01_user_title_wikiproject",
                   self.default_db, self.time_table)
        self.query.run_query(query, self.default_db, "lng_rev_ns01_user_title_wikiproject")

        # check if it's a bot or not
        query = """
            SELECT t1.user_text AS user_text,
                t1.title AS title,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t1.contain_template AS contain_template,
                t1.time_index,
                1 AS is_bot
                FROM `{}.{}` AS t1
                WHERE user_text in (SELECT bot FROM `{}.{}`)
        """.format(self.default_db, "lng_rev_ns01_user_title_wikiproject",
                   self.default_db, "bot_list")
        self.query.run_query(query, self.default_db, "lng_rev_ns01_bots")

        query = """
            SELECT t1.user_text AS user_text,
                t1.title AS title,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t1.contain_template AS contain_template,
                t1.time_index,
                0 AS is_bot
                FROM `{}.{}` AS t1
                WHERE user_text not in (SELECT bot FROM `{}.{}`)
        """.format(self.default_db, "lng_rev_ns01_user_title_wikiproject",
                   self.default_db, "bot_list")
        self.query.run_query(query, self.default_db, "lng_rev_ns01_editors")

        # union the two tables
        query = """
            SELECT *
            FROM `{}.{}`
            UNION ALL
            SELECT *
            FROM `{}.{}`
        """.format(self.default_db, "lng_rev_ns01_bots",
                   self.default_db, "lng_rev_ns01_editors")
        self.query.run_query(query, self.default_db, "lng_rev_ns01")

        ### Longitudinal data for project page edits (ns 45)
        query = """
            SELECT t1.user_text AS user_text,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t1.contain_template AS contain_template,
                t2.index AS time_index
            FROM `{}.{}` AS t1
            CROSS JOIN `{}.{}` AS t2
            WHERE t1.timestamp < t2.ending_time AND t1.timestamp >= t2.starting_time
        """.format(self.default_db, "rev_ns45_user_wikiproject",
                   self.default_db, self.time_table)
        self.query.run_query(query, self.default_db, "lng_rev_ns45_user_title_wikiproject")

        # append bot status
        query = """
            SELECT t1.user_text AS user_text,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t1.contain_template AS contain_template,
                t1.time_index AS time_index,
                1 AS is_bot
            FROM `{}.{}` AS t1
            WHERE t1.user_text in (SELECT bot FROM `{}.{}`)
        """.format(self.default_db, "lng_rev_ns45_user_title_wikiproject",
                   self.default_db, "bot_list")
        self.query.run_query(query, self.default_db, "lng_rev_ns45_bots")

        query = """
            SELECT t1.user_text AS user_text,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t1.contain_template AS contain_template,
                t1.time_index AS time_index,
                0 AS is_bot
            FROM `{}.{}` AS t1
            WHERE t1.user_text not in (SELECT bot FROM `{}.{}`)
        """.format(self.default_db, "lng_rev_ns45_user_title_wikiproject",
                   self.default_db, "bot_list")
        self.query.run_query(query, self.default_db, "lng_rev_ns45_editors")

        # union the two tables
        query = """
            SELECT *
            FROM `{}.{}`
            UNION ALL
            SELECT *
            FROM `{}.{}`
        """.format(self.default_db, "lng_rev_ns45_bots",
                   self.default_db, "lng_rev_ns45_editors")
        self.query.run_query(query, self.default_db, "lng_rev_ns45")


        ### Longitudinal data for project member page edits (ns 23)
        query = """
            SELECT t1.user_text AS user_text,
                t1.page AS member,
                t1.add_template AS add_template,
                t1.ns AS ns,
                t1.contain_template AS contain_template,
                t1.timestamp AS timestamp,
                t2.time_index AS time_index,
                t2.wikiproject AS wikiproject,
                t2.starting_time AS starting_time,
                t2.ending_time AS ending_time
            FROM `{}.{}` AS t1
            INNER JOIN `{}.{}` AS t2
            ON t1.page = t2.user_text
            WHERE t1.timestamp >= t2.starting_time AND t1.timestamp < t2.ending_time
        """.format(self.default_db, "revs23",
                   self.default_db, "user_active_period45")
        self.query.run_query(query, self.default_db, "lng_rev_ns23_member_wikiproject")

        # append bot status
        query = """
            SELECT t1.user_text AS user_text,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t1.contain_template AS contain_template,
                t1.time_index AS time_index,
                1 AS is_bot
            FROM `{}.{}` AS t1
            WHERE t1.user_text in (SELECT bot FROM `{}.{}`)
        """.format(self.default_db, "lng_rev_ns23_member_wikiproject",
                   self.default_db, "bot_list")
        self.query.run_query(query, self.default_db, "lng_rev_ns23_bots")

        query = """
            SELECT t1.user_text AS user_text,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t1.contain_template AS contain_template,
                t1.time_index AS time_index,
                0 AS is_bot
            FROM `{}.{}` AS t1
            WHERE t1.user_text not in (SELECT bot FROM `{}.{}`)
        """.format(self.default_db, "lng_rev_ns23_member_wikiproject",
                   self.default_db, "bot_list")
        self.query.run_query(query, self.default_db, "lng_rev_ns23_editors")

        query = """
            SELECT *
            FROM `{}.{}`
            UNION ALL
            SELECT *
            FROM `{}.{}`
        """.format(self.default_db, "lng_rev_ns23_bots",
                   self.default_db, "lng_rev_ns23_editors")
        self.query.run_query(query, self.default_db, "lng_rev_ns23")

        # label the type of edits on each namespace
        # article type 1
        query = """
            SELECT user_text,
                wikiproject,
                ns,
                timestamp,
                add_template,
                contain_template,
                time_index,
                is_bot,
                1 AS type
                FROM `{}.{}`
        """.format(self.default_db, "lng_rev_ns01")
        self.query.run_query(query, self.default_db, "lng_rev_type_ns01")

        # member type 2
        query = """
            SELECT *,
                2 AS type
                FROM `{}.{}`
        """.format(self.default_db, "lng_rev_ns23")
        self.query.run_query(query, self.default_db, "lng_rev_type_ns23")

        # project type 3
        query = """
            SELECT *,
                3 AS type
                FROM `{}.{}`
        """.format(self.default_db, "lng_rev_ns45")
        self.query.run_query(query, self.default_db, "lng_rev_type_ns45")

        # combine all the edits with template, bot, type info into one table
        query = """
            SELECT *
            FROM `{}.{}`
            UNION ALL
            SELECT *
            FROM `{}.{}`
        """.format(self.default_db, "lng_rev_type_ns01",
                   self.default_db, "lng_rev_type_ns23")
        self.query.run_query(query, self.default_db, "lng_rev_type_ns0123")

        query = """
            SELECT *
            FROM `{}.{}`
            UNION ALL
            SELECT *
            FROM `{}.{}`
        """.format(self.default_db, "lng_rev_type_ns0123",
                   self.default_db, "lng_rev_type_ns45")
        self.query.run_query(query, self.default_db, "lng_rev_type_ns012345")


    def create_variable_types_by_longitudinal_data(self):
        # the number for each time period of each project should be the number of each type

        ## CVs
        # total number of template used
        query = """
            SELECT wikiproject,
                time_index,
                SUM(add_template) AS total_template
                FROM `{}.{}`
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_total_template")

        # project scope
        query = """
            SELECT wikiproject,
                COUNT(*) AS project_scope
                FROM `{}.{}`
                GROUP BY wikiproject
        """.format(self.default_db, "article_projects_2017")
        self.query.run_query(query, self.default_db, "project_scope")

        ## Model 2
        # edits that contains template
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS contain_template
                FROM `{}.{}`
                WHERE contain_template = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_contain_template")

        # edits by bots
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS by_bots
                FROM `{}.{}`
                WHERE is_bot = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_bots")

        ## Model 3
        # template edits by bots
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_bots
                FROM `{}.{}`
                WHERE contain_template = 1 AND is_bot = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_bots_template")

        # template edits by editors
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_editors
                FROM `{}.{}`
                WHERE contain_template = 1 AND is_bot = 0
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_editors_template")

        # non template edits by editors
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS non_template_editors
                FROM `{}.{}`
                WHERE contain_template = 0 AND is_bot = 0
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_editors_non_template")

        # non template edits by editors
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS non_template_bots
                FROM `{}.{}`
                WHERE contain_template = 0 AND is_bot = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_bots_non_template")

        ## Model 4
        # template on article pages
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_article
                FROM `{}.{}`
                WHERE contain_template = 1 AND type = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_template_article")

        # template on member pages
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_member
                FROM `{}.{}`
                WHERE contain_template = 1 AND type = 2
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_template_member")

        # template on project pages
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_project
                FROM `{}.{}`
                WHERE contain_template = 1 AND type = 3
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_template_project")

        ## Model 5
        # template on article pages by bots
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_article_bot
                FROM `{}.{}`
                WHERE contain_template = 1 AND type = 1 AND is_bot = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_template_article_bot")

        # template on member pages by bots
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_member_bot
                FROM `{}.{}`
                WHERE contain_template = 1 AND type = 2 AND is_bot = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_template_member_bot")

        # template on project pages by bots
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_project_bot
                FROM `{}.{}`
                WHERE contain_template = 1 AND type = 3 AND is_bot = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_template_project_bot")

        # template on article pages by editors
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_article_editor
                FROM `{}.{}`
                WHERE contain_template = 1 AND type = 1 AND is_bot = 0
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_template_article_editor")

        # template on member pages by editors
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_member_editor
                FROM `{}.{}`
                WHERE contain_template = 1 AND type = 2 AND is_bot = 0
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_template_member_editor")

        # template on project pages by editors
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS template_project_editor
                FROM `{}.{}`
                WHERE contain_template = 1 AND type = 3 AND is_bot = 0
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_template_project_editor")

        # total edits
        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS total_edits
            FROM `{}.{}`
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_total")

        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS total_edits_bot
            FROM `{}.{}`
            WHERE is_bot = 1
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_total_bot")

        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS total_edits_human
            FROM `{}.{}`
            WHERE is_bot = 0
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_total_human")

        # total edits on article pages for DV
        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS dv_article
            FROM `{}.{}`
            WHERE type = 1
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_dv_article")

        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS dv_article_bot
            FROM `{}.{}`
            WHERE type = 1 AND is_bot = 1
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_dv_article_bot")

        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS dv_article_human
            FROM `{}.{}`
            WHERE type = 1 AND is_bot = 0
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_dv_article_human")

        # total edits on member pages for DV
        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS dv_member
            FROM `{}.{}`
            WHERE type = 2
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_dv_member")

        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS dv_member_bot
            FROM `{}.{}`
            WHERE type = 2 AND is_bot = 1
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_dv_member_bot")

        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS dv_member_human
            FROM `{}.{}`
            WHERE type = 2 AND is_bot = 0
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_dv_member_human")

        # total edits on article pages for DV
        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS dv_project
            FROM `{}.{}`
            WHERE type = 3
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_dv_project")

        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS dv_project_bot
            FROM `{}.{}`
            WHERE type = 3 AND is_bot = 1
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_dv_project_bot")

        query = """
            SELECT wikiproject,
            time_index,
            COUNT(*) AS dv_project_human
            FROM `{}.{}`
            WHERE type = 3 AND is_bot = 0
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_dv_project_human")


        ## Model X - bot edits on different namespaces
        # template on article pages by bots
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS article_bot
                FROM `{}.{}`
                WHERE type = 1 AND is_bot = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_article_bot")

        # template on member pages by bots
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS member_bot
                FROM `{}.{}`
                WHERE type = 2 AND is_bot = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_member_bot")

        # template on project pages by bots
        query = """
            SELECT wikiproject,
                time_index,
                COUNT(*) AS project_bot
                FROM `{}.{}`
                WHERE type = 3 AND is_bot = 1
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_type_ns012345")
        self.query.run_query(query, self.default_db, "lng_edits_project_bot")


    def merging_tables(self):

        # merging DVs
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                IFNULL(t2.dv_article, 0) AS dv_article
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "lng_edits_total",
                   self.default_db, "lng_edits_dv_article")
        self.query.run_query(query, self.default_db, "merging1")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.dv_article AS dv_article,
                t1.total_edits AS total_edits,
                IFNULL(t2.dv_member, 0) AS dv_member
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging1",
                   self.default_db, "lng_edits_dv_member")
        self.query.run_query(query, self.default_db, "merging2")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                IFNULL(t2.dv_project, 0) AS dv_project
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging2",
                   self.default_db, "lng_edits_dv_project")
        self.query.run_query(query, self.default_db, "merging3")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                IFNULL(t2.contain_template, 0) AS contain_template
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging3",
                   self.default_db, "lng_edits_contain_template")
        self.query.run_query(query, self.default_db, "merging4")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                IFNULL(t2.by_bots, 0) AS by_bots
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging4",
                   self.default_db, "lng_edits_bots")
        self.query.run_query(query, self.default_db, "merging5")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                IFNULL(t2.template_bots, 0) AS template_bots
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging5",
                   self.default_db, "lng_edits_bots_template")
        self.query.run_query(query, self.default_db, "merging6")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                IFNULL(t2.template_editors, 0) AS template_editors
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging6",
                   self.default_db, "lng_edits_editors_template")
        self.query.run_query(query, self.default_db, "merging7")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                IFNULL(t2.non_template_editors, 0) AS non_template_editors
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging7",
                   self.default_db, "lng_edits_editors_non_template")
        self.query.run_query(query, self.default_db, "merging8")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                IFNULL(t2.non_template_bots, 0) AS non_template_bots
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging8",
                   self.default_db, "lng_edits_bots_non_template")
        self.query.run_query(query, self.default_db, "merging9")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                IFNULL(t2.template_article, 0) AS template_article
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging9",
                   self.default_db, "lng_edits_template_article")
        self.query.run_query(query, self.default_db, "merging10")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                IFNULL(t2.template_member, 0) AS template_member
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging10",
                   self.default_db, "lng_edits_template_member")
        self.query.run_query(query, self.default_db, "merging11")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                IFNULL(t2.template_project, 0) AS template_project
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging11",
                   self.default_db, "lng_edits_template_project")
        self.query.run_query(query, self.default_db, "merging12")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                IFNULL(t2.template_article_bot, 0) AS template_article_bot
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging12",
                   self.default_db, "lng_edits_template_article_bot")
        self.query.run_query(query, self.default_db, "merging13")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                IFNULL(t2.template_member_bot, 0) AS template_member_bot
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging13",
                   self.default_db, "lng_edits_template_member_bot")
        self.query.run_query(query, self.default_db, "merging14")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                IFNULL(t2.template_project_bot, 0) AS template_project_bot
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging14",
                   self.default_db, "lng_edits_template_project_bot")
        self.query.run_query(query, self.default_db, "merging15")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                IFNULL(t2.template_article_editor, 0) AS template_article_editor
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging15",
                   self.default_db, "lng_edits_template_article_editor")
        self.query.run_query(query, self.default_db, "merging16")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                IFNULL(t2.template_member_editor, 0) AS template_member_editor
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging16",
                   self.default_db, "lng_edits_template_member_editor")
        self.query.run_query(query, self.default_db, "merging17")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                IFNULL(t2.template_project_editor, 0) AS template_project_editor
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging17",
                   self.default_db, "lng_edits_template_project_editor")
        self.query.run_query(query, self.default_db, "merging18")

        # merging CVs
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                IFNULL(t2.total_template, 0) AS cv_total_template
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging18",
                   self.default_db, "lng_edits_total_template")
        self.query.run_query(query, self.default_db, "merging19")

        # merging model X
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                IFNULL(t2.article_bot, 0) AS article_bot
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging19",
                   self.default_db, "lng_edits_article_bot")
        self.query.run_query(query, self.default_db, "merging20")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                IFNULL(t2.member_bot, 0) AS member_bot
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging20",
                   self.default_db, "lng_edits_member_bot")
        self.query.run_query(query, self.default_db, "merging21")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                IFNULL(t2.project_bot, 0) AS project_bot
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging21",
                   self.default_db, "lng_edits_project_bot")
        self.query.run_query(query, self.default_db, "merging22")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t2.project_scope AS cv_project_scope
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject
        """.format(self.default_db, "merging22",
                   self.default_db, "project_scope")
        self.query.run_query(query, self.default_db, "merging23")

        # merging extra CVs
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                IFNULL(t2.active_members, 0) AS active_members
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging23",
                   self.default_db, "cv_active_members")
        self.query.run_query(query, self.default_db, "merging24")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                t1.active_members AS cv_active_members,
                IFNULL(t2.wp_tenure, 0) AS cv_wp_tenure
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.index
        """.format(self.default_db, "merging24",
                   self.default_db, "cv_project_tenure")
        self.query.run_query(query, self.default_db, "merging25")

        # merging bot/human edits as DV
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                IFNULL(t2.total_edits_bot, 0) AS total_edits_bot,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging25",
                   self.default_db, "lng_edits_total_bot")
        self.query.run_query(query, self.default_db, "merging26")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                IFNULL(t2.total_edits_human, 0) AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging26",
                   self.default_db, "lng_edits_total_human")
        self.query.run_query(query, self.default_db, "merging27")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                IFNULL(t2.dv_article_bot, 0) AS total_article_bot,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging27",
                   self.default_db, "lng_edits_dv_article_bot")
        self.query.run_query(query, self.default_db, "merging28")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.total_article_bot AS total_article_bot,
                IFNULL(t2.dv_article_human, 0) AS total_article_human,
                t1.dv_member AS dv_member,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging28",
                   self.default_db, "lng_edits_dv_article_human")
        self.query.run_query(query, self.default_db, "merging29")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.total_article_bot AS total_article_bot,
                t1.total_article_human AS total_article_human,
                t1.dv_member AS dv_member,
                IFNULL(t2.dv_member_bot, 0) AS total_member_bot,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging29",
                   self.default_db, "lng_edits_dv_member_bot")
        self.query.run_query(query, self.default_db, "merging30")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.total_article_bot AS total_article_bot,
                t1.total_article_human AS total_article_human,
                t1.dv_member AS dv_member,
                t1.total_member_bot AS total_member_bot,
                IFNULL(t2.dv_member_human, 0) AS total_member_human,
                t1.dv_project AS dv_project,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging30",
                   self.default_db, "lng_edits_dv_member_human")
        self.query.run_query(query, self.default_db, "merging31")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.total_article_bot AS total_article_bot,
                t1.total_article_human AS total_article_human,
                t1.dv_member AS dv_member,
                t1.total_member_bot AS total_member_bot,
                t1.total_member_human AS total_member_human,
                t1.dv_project AS dv_project,
                IFNULL(t2.dv_project_bot, 0) AS total_project_bot,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging31",
                   self.default_db, "lng_edits_dv_project_bot")
        self.query.run_query(query, self.default_db, "merging32")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.total_article_bot AS dv_article_bot,
                t1.total_article_human AS dv_article_human,
                t1.dv_member AS dv_member,
                t1.total_member_bot AS dv_member_bot,
                t1.total_member_human AS dv_member_human,
                t1.dv_project AS dv_project,
                t1.total_project_bot AS dv_project_bot,
                IFNULL(t2.dv_project_human, 0) AS dv_project_human,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure
                FROM `{}.{}` AS t1
                LEFT JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "merging32",
                   self.default_db, "lng_edits_dv_project_human")
        self.query.run_query(query, self.default_db, "merging_final")


    def compute_variables(self):

        # only valid wikiprojects
        query = """
            SELECT t1.wikiproject AS wikiproject,
                t2.nwikiproject AS nwikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.dv_article_bot AS dv_article_bot,
                t1.dv_article_human AS dv_article_human,
                t1.dv_member AS dv_member,
                t1.dv_member_bot AS dv_member_bot,
                t1.dv_member_human AS dv_member_human,
                t1.dv_project AS dv_project,
                t1.dv_project_bot AS dv_project_bot,
                t1.dv_project_human AS dv_project_human,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject
        """.format(self.default_db, "merging_final",
                   self.default_db, "valid_nwikiprojects")
        self.query.run_query(query, self.default_db, "compute1")

        # compute IVs
        query = """
            SELECT *,
                time_index+1 AS next_time,
                time_index-1 AS pre_time
                FROM `{}.{}`
        """.format(self.default_db, "compute1")
        self.query.run_query(query, self.default_db, "compute2")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.nwikiproject AS nwikiproject,
                t1.time_index AS time_index,
                t1.pre_time AS pre_time,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.dv_article_bot AS dv_article_bot,
                t1.dv_article_human AS dv_article_human,
                t1.dv_member AS dv_member,
                t1.dv_member_bot AS dv_member_bot,
                t1.dv_member_human AS dv_member_human,
                t1.dv_project AS dv_project,
                t1.dv_project_bot AS dv_project_bot,
                t1.dv_project_human AS dv_project_human,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t2.total_edits AS next_total,
                t2.total_edits_bot AS next_total_bot,
                t2.total_edits_human AS next_total_human,
                t2.dv_article AS next_dv_article,
                t2.dv_article_bot AS next_dv_article_bot,
                t2.dv_article_human AS next_dv_article_human,
                t2.dv_member AS next_dv_member,
                t2.dv_member_bot AS next_dv_member_bot,
                t2.dv_member_human AS next_dv_member_human,
                t2.dv_project AS next_dv_project,
                t2.dv_project_bot AS next_dv_project_bot,
                t2.dv_project_human AS next_dv_project_human,
                t1.cv_total_template AS cv_total_template,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.next_time = t2.time_index
        """.format(self.default_db, "compute2",
                   self.default_db, "compute2")
        self.query.run_query(query, self.default_db, "compute3")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.nwikiproject AS nwikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.dv_article_bot AS dv_article_bot,
                t1.dv_article_human AS dv_article_human,
                t1.dv_member AS dv_member,
                t1.dv_member_bot AS dv_member_bot,
                t1.dv_member_human AS dv_member_human,
                t1.dv_project AS dv_project,
                t1.dv_project_bot AS dv_project_bot,
                t1.dv_project_human AS dv_project_human,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.next_total AS next_total,
                t1.next_total_bot AS next_total_bot,
                t1.next_total_human AS next_total_human,
                t1.next_dv_article AS next_dv_article,
                t1.next_dv_article_bot AS next_dv_article_bot,
                t1.next_dv_article_human AS next_dv_article_human,
                t1.next_dv_member AS next_dv_member,
                t1.next_dv_member_bot AS next_dv_member_bot,
                t1.next_dv_member_human AS next_dv_member_human,
                t1.next_dv_project AS next_dv_project,
                t1.next_dv_project_bot AS next_dv_project_bot,
                t1.next_dv_project_human AS next_dv_project_human,
                t1.cv_total_template AS cv_total_template,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure,
                t2.total_edits AS cv_pre_edits,
                t2.dv_article AS cv_pre_article,
                t2.dv_member AS cv_pre_member,
                t2.dv_project AS cv_pre_project,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.pre_time = t2.time_index
        """.format(self.default_db, "compute3",
                   self.default_db, "compute3")
        self.query.run_query(query, self.default_db, "compute4")

        query = """
            SELECT wikiproject,
                nwikiproject,
                time_index,
                total_edits,
                total_edits_bot,
                total_edits_human,
                dv_article,
                dv_article_bot,
                dv_article_human,
                dv_member,
                dv_member_bot,
                dv_member_human,
                dv_project,
                dv_project_bot,
                dv_project_human,
                next_total,
                next_total_bot,
                next_total_human,
                next_dv_article,
                next_dv_article_bot,
                next_dv_article_human,
                next_dv_member,
                next_dv_member_bot,
                next_dv_member_human,
                next_dv_project,
                next_dv_project_bot,
                next_dv_project_human,
                contain_template,
                by_bots,
                template_bots,
                template_editors,
                non_template_editors,
                non_template_bots,
                template_article,
                template_member,
                template_project,
                template_article_bot,
                template_member_bot,
                template_project_bot,
                template_article_editor,
                template_member_editor,
                template_project_editor,
                cv_total_template,
                cv_project_scope,
                cv_pre_edits,
                cv_pre_article,
                cv_pre_member,
                cv_pre_project,
                cv_active_members,
                cv_wp_tenure,
                (1.0 * contain_template / (total_edits+1)) AS pct_contain_template,
                (1.0 * by_bots / (total_edits+1)) AS pct_by_bots,
                (1.0 * template_bots / (total_edits+1)) AS pct_template_bots,
                (1.0 * template_editors / (total_edits+1)) AS pct_template_editors,
                (1.0 * non_template_bots / (total_edits+1)) AS pct_non_template_bots,
                (1.0 * non_template_editors / (total_edits+1)) AS pct_non_template_editors,
                (1.0 * template_article / (dv_article+1)) AS pct_template_article,
                (1.0 * template_member / (dv_member+1)) AS pct_template_member,
                (1.0 * template_project / (dv_project+1)) AS pct_template_project,
                (1.0 * template_article_bot / (dv_article+1)) AS pct_template_article_bot,
                (1.0 * template_member_bot / (dv_member+1)) AS pct_template_member_bot,
                (1.0 * template_project_bot / (dv_project+1)) AS pct_template_project_bot,
                (1.0 * template_article_editor / (total_edits+1)) AS pct_template_article_editor,
                (1.0 * template_member_editor / (total_edits+1)) AS pct_template_member_editor,
                (1.0 * template_project_editor / (total_edits+1)) AS pct_template_project_editor,
                (1.0 * (next_total-total_edits) / (total_edits+1)) AS pct_dv_total,
                (1.0 * (next_total_bot-total_edits_bot) / (total_edits_bot+1)) AS pct_dv_total_bot,
                (1.0 * (next_total_human-total_edits_human) / (total_edits_human+1)) AS pct_dv_total_human,
                (1.0 * (next_dv_article-dv_article) / (dv_article+1)) AS pct_dv_article,
                (1.0 * (next_dv_article_bot-dv_article_bot) / (dv_article_bot+1)) AS pct_dv_article_bot,
                (1.0 * (next_dv_article_human-dv_article_human) / (dv_article_human+1)) AS pct_dv_article_human,
                (1.0 * (next_dv_member-dv_member) / (dv_member+1)) AS pct_dv_member,
                (1.0 * (next_dv_member_bot-dv_member_bot) / (dv_member_bot+1)) AS pct_dv_member_bot,
                (1.0 * (next_dv_member_human-dv_member) / (dv_member_human+1)) AS pct_dv_member_human,
                (1.0 * (next_dv_project-dv_project) / (dv_project+1)) AS pct_dv_project,
                (1.0 * (next_dv_project_bot-dv_project_bot) / (dv_project_bot+1)) AS pct_dv_project_bot,
                (1.0 * (next_dv_project_human-dv_project_human) / (dv_project_human+1)) AS pct_dv_project_human,
                article_bot,
                member_bot,
                project_bot,
                (1.0 * article_bot / (dv_article+1)) AS pct_article_bot,
                (1.0 * member_bot / (dv_member+1)) AS pct_member_bot,
                (1.0 * project_bot / (dv_project+1)) AS pct_project_bot
                FROM `{}.{}`
                ORDER BY wikiproject, time_index ASC
        """.format(self.default_db, "compute4")
        self.query.run_query(query, self.default_db, "compute5")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.nwikiproject AS nwikiproject,
                t1.time_index AS time_index,
                t1.total_edits AS total_edits,
                t1.total_edits_bot AS total_edits_bot,
                t1.total_edits_human AS total_edits_human,
                t1.dv_article AS dv_article,
                t1.dv_article_bot AS dv_article_bot,
                t1.dv_article_human AS dv_article_human,
                t1.dv_member AS dv_member,
                t1.dv_member_bot AS dv_member_bot,
                t1.dv_member_human AS dv_member_human,
                t1.dv_project AS dv_project,
                t1.dv_project_bot AS dv_project_bot,
                t1.dv_project_human AS dv_project_human,
                t1.next_total AS next_total,
                t1.next_total_bot AS next_total_bot,
                t1.next_total_human AS next_total_human,
                t1.next_dv_article AS next_dv_article,
                t1.next_dv_article_bot AS next_dv_article_bot,
                t1.next_dv_article_human AS next_dv_article_human,
                t1.next_dv_member AS next_dv_member,
                t1.next_dv_member_bot AS next_dv_member_bot,
                t1.next_dv_member_human AS next_dv_member_human,
                t1.next_dv_project AS next_dv_project,
                t1.next_dv_project_bot AS next_dv_project_bot,
                t1.next_dv_project_human AS next_dv_project_human,
                t1.contain_template AS contain_template,
                t1.by_bots AS by_bots,
                t1.template_bots AS template_bots,
                t1.template_editors AS template_editors,
                t1.non_template_editors AS non_template_editors,
                t1.non_template_bots AS non_template_bots,
                t1.template_article AS template_article,
                t1.template_member AS template_member,
                t1.template_project AS template_project,
                t1.template_article_bot AS template_article_bot,
                t1.template_member_bot AS template_member_bot,
                t1.template_project_bot AS template_project_bot,
                t1.template_article_editor AS template_article_editor,
                t1.template_member_editor AS template_member_editor,
                t1.template_project_editor AS template_project_editor,
                t1.cv_total_template AS cv_total_template,
                t1.cv_project_scope AS cv_project_scope,
                t1.cv_pre_edits AS cv_pre_edits,
                t1.cv_pre_article AS cv_pre_article,
                t1.cv_pre_member AS cv_pre_member,
                t1.cv_pre_project AS cv_pre_project,
                t1.cv_active_members AS cv_active_members,
                t1.cv_wp_tenure AS cv_wp_tenure,
                t1.pct_contain_template AS pct_contain_template,
                t1.pct_by_bots AS pct_by_bots,
                t1.pct_template_bots AS pct_template_bots,
                t1.pct_template_editors AS pct_template_editors,
                t1.pct_non_template_bots AS pct_non_template_bots,
                t1.pct_non_template_editors AS pct_non_template_editors,
                t1.pct_template_article AS pct_template_article,
                t1.pct_template_member AS pct_template_member,
                t1.pct_template_project AS pct_template_project,
                t1.pct_template_article_bot AS pct_template_article_bot,
                t1.pct_template_member_bot AS pct_template_member_bot,
                t1.pct_template_project_bot AS pct_template_project_bot,
                t1.pct_template_article_editor AS pct_template_article_editor,
                t1.pct_template_member_editor AS pct_template_member_editor,
                t1.pct_template_project_editor AS pct_template_project_editor,
                t1.pct_dv_total AS pct_dv_total,
                t1.pct_dv_total_bot AS pct_dv_total_bot,
                t1.pct_dv_total_human AS pct_dv_total_human,
                t1.pct_dv_article AS pct_dv_article,
                t1.pct_dv_article_bot AS pct_dv_article_bot,
                t1.pct_dv_article_human AS pct_dv_article_human,
                t1.pct_dv_member AS pct_dv_member,
                t1.pct_dv_member_bot AS pct_dv_member_bot,
                t1.pct_dv_member_human AS pct_dv_member_human,
                t1.pct_dv_project AS pct_dv_project,
                t1.pct_dv_project_bot AS pct_dv_project_bot,
                t1.pct_dv_project_human AS pct_dv_project_human,
                t1.article_bot AS article_bot,
                t1.member_bot AS member_bot,
                t1.project_bot AS project_bot,
                t1.pct_article_bot AS pct_article_bot,
                t1.pct_member_bot AS pct_member_bot,
                t1.pct_project_bot AS pct_project_bot,
                t2.delta_quality AS delta_quality
                FROM `{}.{}` AS t1
                INNER JOIN `{}.{}` AS t2
                ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.index
                ORDER BY wikiproject, time_index ASC
        """.format(self.default_db, "compute5",
                   self.default_db, "article_quality_final")
        self.query.run_query(query, self.default_db, "automation_final_table")


    def stats(self):
        query = """
            SELECT time_index,
                SUM(total_edits) AS total_edits,
                SUM(dv_article) AS total_article,
                SUM(dv_member) AS total_member,
                SUM(dv_project) AS total_project,
                SUM(contain_template) AS total_contain_template,
                SUM(by_bots) AS total_by_bots,
                SUM(template_bots) AS total_template_bots,
                SUM(template_editors) AS total_template_editors,
                SUM(non_template_editors) AS total_non_template_editors,
                SUM(non_template_bots) AS total_non_template_bots,
                SUM(template_article) AS total_template_article,
                SUM(template_member) AS total_template_member,
                SUM(template_project) AS total_template_project,
                SUM(template_article_bot) AS total_template_article_bot,
                SUM(template_member_bot) AS total_template_member_bot,
                SUM(template_project_bot) AS total_template_project_bot,
                SUM(template_article_editor) AS total_template_article_editor,
                SUM(template_member_editor) AS total_template_member_editor,
                SUM(template_project_editor) AS total_template_project_editor,
                SUM(cv_total_template) AS total_template
                FROM `{}.{}`
                GROUP BY time_index
                ORDER BY time_index ASC
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "stats1")

        query = """
            SELECT time_index,
                total_article / (total_edits+1) AS pct_article,
                total_member / (total_edits+1) AS pct_member,
                total_project / (total_edits+1) AS pct_project,
                total_contain_template / (total_edits+1) AS pct_contain_template,
                total_by_bots / (total_edits+1) AS pct_by_bots,
                total_template_bots / (total_edits+1) AS pct_template_bots,
                total_template_editors / (total_edits+1) AS pct_template_editors,
                total_non_template_editors / (total_edits+1) AS pct_non_template_editors,
                total_non_template_bots / (total_edits+1) AS pct_non_template_bots,
                total_template_article / (total_edits+1) AS pct_template_article,
                total_template_member / (total_edits+1) AS pct_template_member,
                total_template_project / (total_edits+1) AS pct_template_project,
                total_template_article_bot / (total_edits+1) AS pct_template_article_bot,
                total_template_member_bot / (total_edits+1) AS pct_template_member_bot,
                total_template_project_bot / (total_edits+1) AS pct_template_project_bot,
                total_template_article_editor / (total_edits+1) AS pct_template_article_editor,
                total_template_member_editor / (total_edits+1) AS pct_template_member_editor,
                total_template_project_editor / (total_edits+1) AS pct_template_project_editor,
                total_template
                FROM `{}.{}`
                ORDER BY time_index ASC
        """.format(self.default_db, "stats1")
        self.query.run_query(query, self.default_db, "stats2")

        query = """
            SELECT wikiproject,
                COUNT(*) AS active_periods,
                SUM(total_edits) AS total_edits,
                SUM(dv_article) AS total_article,
                SUM(dv_member) AS total_member,
                SUM(dv_project) AS total_project,
                SUM(contain_template) AS total_contain_template,
                SUM(by_bots) AS total_by_bots,
                SUM(template_bots) AS total_template_bots,
                SUM(template_editors) AS total_template_editors,
                SUM(non_template_editors) AS total_non_template_editors,
                SUM(non_template_bots) AS total_non_template_bots,
                SUM(template_article) AS total_template_article,
                SUM(template_member) AS total_template_member,
                SUM(template_project) AS total_template_project,
                SUM(template_article_bot) AS total_template_article_bot,
                SUM(template_member_bot) AS total_template_member_bot,
                SUM(template_project_bot) AS total_template_project_bot,
                SUM(template_article_editor) AS total_template_article_editor,
                SUM(template_member_editor) AS total_template_member_editor,
                SUM(template_project_editor) AS total_template_project_editor,
                SUM(cv_total_template) AS total_template
                FROM `{}.{}`
                GROUP BY wikiproject
                ORDER BY total_edits DESC
        """.format(self.default_db, "automation_final_table")
        self.query.run_query(query, self.default_db, "stats3")


    def template_related_generation(self):
        # CV: total number of templates used per project per time period
        query = """
            SELECT wikiproject,
                time_index,
                SUM(add_template) AS total_template
            FROM `{}.{}`
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns01")
        self.query.run_query(query, self.default_db, "lng_rev_ns01_templates")

        query = """
            SELECT wikiproject,
                time_index,
                SUM(add_template) AS total_template
            FROM `{}.{}`
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns45")
        self.query.run_query(query, self.default_db, "lng_rev_ns45_templates")

        query = """
            SELECT *
            FROM `{}.{}`
            UNION ALL
            SElECT *
            FROM `{}.{}`
        """.format(self.default_db, "lng_rev_ns01_templates",
                   self.default_db, "lng_rev_ns45_templates")
        self.query.run_query(query, self.default_db, "lng_rev_ns0145_templates_temp")

        query = """
            SELECT wikiproject,
                time_index,
                SUM(total_template) AS total_template
            FROM `{}.{}`
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns0145_templates_temp")
        self.query.run_query(query, self.default_db, "lng_rev_ns0145_templates")

        # todo: add on member page templates to the above table


        # templates used by bots or editors on different pages

        # on article pages - ns 0 and 1
        query = """
            SELECT wikiproject,
                time_index,
                SUM(add_template) AS bot_template
            FROM `{}.{}`
            WHERE is_bot = 1
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns01")
        self.query.run_query(query, self.default_db, "lng_rev_ns01_template_bot")

        query = """
            SELECT wikiproject,
                time_index,
                SUM(add_template) AS editor_template
            FROM `{}.{}`
            WHERE is_bot = 0
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns01")
        self.query.run_query(query, self.default_db, "lng_rev_ns01_template_editor")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                IFNULL(t1.bot_template, 0) AS bot_template01,
                IFNULL(t2.editor_template, 0) AS editor_template01
            FROM `{}.{}` AS t1
            FULL OUTER JOIN `{}.{}` AS t2
            ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "lng_rev_ns01_template_bot",
                   self.default_db, "lng_rev_ns01_template_editor")
        self.query.run_query(query, self.default_db, "lng_rev_ns01_template")


        # on project pages - ns 4 and 5
        query = """
            SELECT wikiproject,
                time_index,
                SUM(add_template) AS bot_template
            FROM `{}.{}`
            WHERE is_bot = 1
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns45")
        self.query.run_query(query, self.default_db, "lng_rev_ns45_template_bot")

        query = """
            SELECT wikiproject,
                time_index,
                SUM(add_template) AS editor_template
            FROM `{}.{}`
            WHERE is_bot = 0
            GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns45")
        self.query.run_query(query, self.default_db, "lng_rev_ns45_template_editor")

        query = """
            SELECT t1.wikiproject AS wikiproject,
                t1.time_index AS time_index,
                IFNULL(t1.bot_template, 0) AS bot_template45,
                IFNULL(t2.editor_template, 0) AS editor_template45
            FROM `{}.{}` AS t1
            FULL OUTER JOIN `{}.{}` AS t2
            ON t1.wikiproject = t2.wikiproject AND t1.time_index = t2.time_index
        """.format(self.default_db, "lng_rev_ns45_template_bot",
                   self.default_db, "lng_rev_ns45_template_editor")
        self.query.run_query(query, self.default_db, "lng_rev_ns45_template")

        # todo: add the part for member page template

        # total number of templates used by bots all pages
        query = """
            SELECT *
            FROM `{}.{}`
            UNION ALL
            SElECT *
            FROM `{}.{}`
        """.format(self.default_db, "lng_rev_ns01_template_bot",
                   self.default_db, "lng_rev_ns45_template_bot")
        self.query.run_query(query, self.default_db, "lng_rev_ns0145_template_bot_temp")

        query = """
            SELECT wikiproject,
                time_index,
                SUM(bot_template) AS bot_template
                FROM `{}.{}`
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns0145_template_bot_temp")
        self.query.run_query(query, self.default_db, "lng_rev_ns0145_template_bot")

        # todo add member pages
        # total number of templates used by editors all pages
        query = """
            SELECT *
            FROM `{}.{}`
            UNION ALL
            SElECT *
            FROM `{}.{}`
        """.format(self.default_db, "lng_rev_ns01_template_editor",
                   self.default_db, "lng_rev_ns45_template_editor")
        self.query.run_query(query, self.default_db, "lng_rev_ns0145_template_editor_temp")

        query = """
            SELECT wikiproject,
                time_index,
                SUM(editor_template) AS editor_template
                FROM `{}.{}`
                GROUP BY wikiproject, time_index
        """.format(self.default_db, "lng_rev_ns0145_template_editor_temp")
        self.query.run_query(query, self.default_db, "lng_rev_ns0145_template_editor")

        # todo add member pages



    def select_valid_projects(self):
        # need to do this when seeing the new data
        # # WikiProject Name Cleaning TODO: run this manually
        # query = """
        #     SELECT title,
        #            (FIRST(SPLIT(wikiproject, '1='))) AS wikiproject,
        #            pageId,
        #            importance,
        #            class
        #     FROM(SELECT title,
        #                (FIRST(SPLIT(wikiproject, '{{'))) AS wikiproject,
        #                pageId,
        #                importance,
        #                class
        #         FROM (SELECT title,
        #                      (FIRST(SPLIT(wikiproject, '<!--'))) AS wikiproject,
        #                      pageId,
        #                      importance,
        #                      class
        #               FROM (SELECT title,
        #                            (REPLACE(wikiproject, "#", "")) AS wikiproject,
        #                            pageId,
        #                            importance,
        #                            class
        #                     FROM (SELECT title,
        #                                   (REPLACE(wikiproject, "hawaii)", "hawaii")) AS wikiproject,
        #                                   pageId,
        #                                   importance,
        #                                   class
        #                            FROM (SELECT title,
        #                                        (REPLACE(wikiproject, "_", " ")) AS wikiproject,
        #                                        pageId,
        #                                        importance,
        #                                        class
        #                                 FROM `{}.{}`)))))
        #     WHERE wikiproject <> 'requested articles' AND wikiproject <> 'articles for creation' AND wikiproject <> 'articles for creation (admin)' AND wikiproject <> 'deletion'
        # """.format(self.default_db, "article_projects_2017")
        # self.query.run_query(query, self.default_db, "valid_wikiprojects_cleaned")

        # add more project selection criteria if needed, like members greater than 3, etc


        # skip collaborative projects

        pass

    # total number of edits on project member pages per time period
    def create_edits_on_project_member_pages(self):
        # user's active period on the project
        query = """
            SELECT user_text,
                wikiproject,
                MIN(timestamp) AS starting_ts,
                MAX(timestamp) AS ending_ts
                FROM `{}.{}`
                GROUP BY user_text, wikiproject
        """.format(self.default_db, "rev_ns45_user_wikiproject")
        self.query.run_query(query, self.default_db, "user_active_timepoints45")

        # locate project members in time period
        query = """
            SELECT t1.user_text AS user_text,
                t1.wikiproject AS wikiproject,
                t2.index AS time_index,
                t2.starting_time AS starting_time,
                t2.ending_time AS ending_time
                FROM `{}.{}` AS t1
                CROSS JOIN `{}.{}` AS t2
                WHERE t1.starting_ts <= t2.ending_time AND t1.ending_ts > t2.starting_time
        """.format(self.default_db, "user_active_timepoints45",
                   self.default_db, self.time_table)
        self.query.run_query(query, self.default_db, "user_active_period45")

        # identify edits on project members
        query = """
            SELECT rev_user_text AS user_text,
                rev_user_id AS user_id,
                rev_timestamp AS timestamp,
                add_template,
                3 AS ns,
                IF(add_template != 0, 1, 0) AS contain_template,
                REPLACE(rev_page_title, "User talk:", "") AS page
            FROM `{}.{}`
            WHERE ns = 3
        """.format(self.default_db, self.raw_revs)
        self.query.run_query(query, self.default_db, "revs3")

        query = """
            SELECT rev_user_text AS user_text,
                rev_user_id AS user_id,
                rev_timestamp AS timestamp,
                add_template,
                2 AS ns,
                IF(add_template != 0, 1, 0) AS contain_template,
                REPLACE(rev_page_title, "User:", "") AS page
            FROM `{}.{}`
            WHERE ns = 2
        """.format(self.default_db, self.raw_revs)
        self.query.run_query(query, self.default_db, "revs2")

        query = """
            SELECT *
            FROM `{}`.revs2
            UNION ALL
            SELECT *
            FROM `{}`.revs3
        """.format(self.default_db, self.default_db)
        self.query.run_query(query, self.default_db, "revs23")


    def create_edits_on_project_article_pages(self):

        # find all revisions on ns 0
        query = """
            SELECT LOWER(rev_page_title) AS page_title,
                   rev_user_text AS user_text,
                   ns,
                   rev_timestamp AS timestamp,
                   add_template
            FROM `{}.{}`
            WHERE ns = 0
        """.format(self.default_db, self.raw_revs)
        self.query.run_query(query, self.default_db, "rev_lower_ns0")

        # find all revisions on ns 1
        query = """
            SELECT REPLACE(LOWER(rev_page_title), "talk:", "") AS page_title,
                   rev_user_text AS user_text,
                   ns,
                   rev_timestamp AS timestamp,
                   add_template
            FROM `{}.{}`
            WHERE ns = 1
        """.format(self.default_db, self.raw_revs)
        self.query.run_query(query, self.default_db, "rev_lower_ns1")

        # connect edits on project pages
        query = """
            SELECT rev.user_text AS user_text,
                   rev.page_title AS title,
                   rev.ns AS ns,
                   rev.timestamp AS timestamp,
                   rev.add_template AS add_template,
                   IF(rev.add_template != 0, 1, 0) AS contain_template,
                   art_wp.wikiproject AS wikiproject
            FROM `{}.{}` AS rev
            INNER JOIN `{}.{}` AS art_wp
              ON rev.page_title = art_wp.title
        """.format(self.default_db, "rev_lower_ns0",
                   self.default_db, self.article_projects_table)
        self.query.run_query(query, self.default_db, "rev_ns0_user_title_wikiproject")

        query = """
            SELECT rev.user_text AS user_text,
                   rev.page_title AS title,
                   rev.ns AS ns,
                   rev.timestamp AS timestamp,
                   rev.add_template AS add_template,
                   IF(rev.add_template != 0, 1, 0) AS contain_template,
                   art_wp.wikiproject AS wikiproject
            FROM `{}.{}` AS rev
            INNER JOIN `{}.{}` AS art_wp
              ON rev.page_title = art_wp.title
        """.format(self.default_db, "rev_lower_ns1",
                   self.default_db, self.article_projects_table)
        self.query.run_query(query, self.default_db, "rev_ns1_user_title_wikiproject")

        # union the tables
        query = """
            SELECT *
            FROM `{}`.rev_ns0_user_title_wikiproject
            UNION ALL
            SELECT *
            FROM `{}`.rev_ns1_user_title_wikiproject
        """.format(self.default_db, self.default_db)
        self.query.run_query(query, self.default_db, "rev_ns01_user_title_wikiproject")


    def create_edits_on_project_pages(self):

        # separate project related pages from the raw data
        query = """
            SELECT rev_user_text AS user_text,
               rev_user_id AS user_id,
               rev_page_title AS wikiproject_page,
               COUNT(*) AS amount,
               MIN(rev_timestamp) AS first_edit,
               CAST(AVG(ns) AS INT64) as ns
            FROM `{}.{}`
            WHERE ns = 4 OR ns = 5
            GROUP BY user_text, user_id, wikiproject_page
        """.format(self.default_db, self.raw_revs)
        self.query.run_query(query, self.default_db, "user_edits_on_wikiproject_page")

        # # work on ns 4 for project page #todo function not running.. need to run manually..
        # query = """
        #     SELECT user_text,
        #        user_id,
        #        (LOWER(wikiproject_page)) AS wikiproject_page,
        #        origin_wikiproject_page,
        #        amount,
        #        first_edit,
        #        ns
        #     FROM(SELECT user_text,
        #                 user_id,
        #                FIRST(SPLIT(wikiproject_page, '/')) AS wikiproject_page,
        #                origin_wikiproject_page,
        #                amount,
        #                first_edit,
        #                ns
        #          FROM (SELECT user_text,
        #                       user_id,
        #                       (REPLACE(wikiproject_page, 'Wikipedia:', '')) AS wikiproject_page,
        #                       origin_wikiproject_page,
        #                       amount,
        #                       first_edit,
        #                       ns
        #                FROM (SELECT user_text,
        #                             user_id,
        #                             (REPLACE(wikiproject_page, 'Wikipedia:WikiProject ', '')) AS wikiproject_page,
        #                             wikiproject_page AS origin_wikiproject_page,
        #                             amount,
        #                             first_edit,
        #                             ns
        #                      FROM (SELECT * FROM `{}.{}` WHERE ns = 4))))
        # """.format(self.default_db, "user_edits_on_wikiproject_page")
        # self.query.run_query(query, self.default_db, "wikiproject_ns4_names")
        #
        # # work on ns 5 for project page #todo function not running.. need to run manually..
        # query = """
        #     SELECT user_text,
        #            user_id,
        #            (LOWER(wikiproject_page)) AS wikiproject_page,
        #            origin_wikiproject_page,
        #            amount,
        #            first_edit,
        #            ns
        #     FROM(SELECT user_text,
        #                 user_id,
        #                (FIRST(SPLIT(wikiproject_page, '/'))) AS wikiproject_page,
        #                origin_wikiproject_page,
        #                amount,
        #                first_edit,
        #                ns
        #          FROM (SELECT user_text,
        #                       user_id,
        #                       (REPLACE(wikiproject_page, 'Wikipedia talk:', '')) AS wikiproject_page,
        #                       origin_wikiproject_page,
        #                       amount,
        #                       first_edit,
        #                       ns
        #                FROM (SELECT user_text,
        #                             user_id,
        #                             (REPLACE(wikiproject_page, 'Wikipedia talk:WikiProject ', '')) AS wikiproject_page,
        #                             wikiproject_page AS origin_wikiproject_page,
        #                             amount,
        #                             first_edit,
        #                             ns
        #                      FROM (SELECT * FROM `{}.{}` WHERE ns = 5))))
        # """.format(self.default_db, "user_edits_on_wikiproject_page")
        # self.query.run_query(query, self.default_db, "wikiproject_ns5_names")

        # combine project pages (edits include bots and regular users, not IP users)
        query = """
            SELECT *
            FROM `{}`.wikiproject_ns4_names
            UNION ALL
            SELECT *
            FROM `{}`.wikiproject_ns5_names
        """.format(self.default_db, self.default_db)
        self.query.run_query(query, self.default_db, "project_pages_45")

        query = """
                SELECT user_text,
                       user_id,
                       wikiproject_page,
                       origin_wikiproject_page,
                       SUM(amount) AS total_edits,
                       MIN(first_edit) AS first_edit
                FROM `{}.{}`
                GROUP BY user_text, user_id, wikiproject_page, origin_wikiproject_page
        """.format(self.default_db, "project_pages_45")
        self.query.run_query(query, self.default_db, "user_first_project_page_edit")

        # only remain valid projects
        query = """
                SELECT t1.user_text AS user_text,
                       t1.user_id AS user_id,
                       t1.wikiproject_page AS wikiproject_page,
                       t1.origin_wikiproject_page AS origin_wikiproject_page,
                       t1.total_edits AS total_edits,
                       t1.first_edit AS first_edit
                FROM `{}.{}` t1
                INNER JOIN `{}.{}` t2
                ON t1.wikiproject_page = t2.wikiproject
        """.format(self.default_db, "user_first_project_page_edit",
                   self.default_db, self.valid_projects)
        self.query.run_query(query, self.default_db, "user_first_project_page_edit_valid_projects")

        # get all revs on related ns 4 and 5
        query = """
            SELECT t1.user_text AS user_text,
                    t1.user_id AS user_id,
                    t1.wikiproject_page AS wikiproject,
                    t2.add_template AS add_template,
                    IF(t2.add_template != 0, 1, 0) AS contain_template,
                    t2.rev_timestamp AS timestamp,
                    t2.ns AS ns
            FROM `{}.{}` t1
            INNER JOIN `{}.{}` t2
            ON t1.user_text = t2.rev_user_text AND t1.origin_wikiproject_page = t2.rev_page_title
        """.format(self.default_db, "user_first_project_page_edit_valid_projects",
                   self.default_db, self.raw_revs)
        self.query.run_query(query, self.default_db, "rev_ns45_user_wikiproject")

def main():

    exe = Executor()
    exe.main()
    # exe.stats()

main()