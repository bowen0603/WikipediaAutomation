__author__ = 'bobo'
from GQuerier import QueryHandler


class Executor:

    # need to run manually: create_edits_on_project_pages()

    def __init__(self):
        self.query = QueryHandler()
        self.default_db = "bowen_wikipedia_raw" #"bowen_automation"
        self.raw_revs = "revs_2017_temp" #"revs_2017"

        # variables to change
        self.article_projects_table = "article_projects"
        self.valid_projects = "valid_wikiprojects"

    def main(self):
        # self.create_edits_on_project_pages()

        self.select_valid_projects()

        # todo: self.select_bots()

        # self.create_edits_on_project_article_pages()

        # self.create_longitudinal_data()

        self.generate_longitudinal_IVDVs()

        # self.template_related_generation()

        # generate DVs

        # clear project article relationship for project productivity

        # have project page info ready -


        # having project member page info ready

        # have a longitudinal data set ready
        # generate longtidinal dataset

        # generate IVs

        # generate CVs

        # merge IVs and DVs

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

        # the time period table is ready

        # join time table with article edits
        query = """
            SELECT t1.user_text AS user_text,
                t1.title AS title,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t2.index AS time_index
            FROM `{}.{}` AS t1
            CROSS JOIN `{}.{}` AS t2
            WHERE t1.timestamp < t2.ending_time AND t1.timestamp >= t2.starting_time
        """.format(self.default_db, "rev_ns01_user_title_wikiproject",
                   self.default_db, "time_index")
        self.query.run_query(query, self.default_db, "lng_rev_ns01_user_title_wikiproject")

        # check if it's a bot or not
        query = """
            SELECT t1.user_text AS user_text,
                t1.title AS title,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
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

        # join time table with project edits
        query = """
            SELECT t1.user_text AS user_text,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
                t2.index AS time_index
            FROM `{}.{}` AS t1
            CROSS JOIN `{}.{}` AS t2
            WHERE t1.timestamp < t2.ending_time AND t1.timestamp >= t2.starting_time
        """.format(self.default_db, "rev_ns45_user_wikiproject",
                   self.default_db, "time_index")
        self.query.run_query(query, self.default_db, "lng_rev_ns45_user_title_wikiproject")

        # append bot status
        query = """
            SELECT t1.user_text AS user_text,
                t1.wikiproject AS wikiproject,
                t1.ns AS ns,
                t1.timestamp AS timestamp,
                t1.add_template AS add_template,
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


        #todo: edits/template on member pages

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
                IFNULL(t1.bot_template, 0) AS bot_template45,
                IFNULL(t2.editor_template, 0) AS editor_template45
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

main()