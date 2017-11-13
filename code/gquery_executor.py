__author__ = 'bobo'
from GQuerier import QueryHandler


class Executor:

    # need to run manually: create_project_pages()

    def __init__(self):
        self.query = QueryHandler()
        self.default_db = "bowen_wikipedia_raw" #"bowen_automation"
        self.raw_revs = "revs_2015" #"revs_2017"

    def main(self):
        self.create_project_pages()
        # generate DVs

        # clear project article relationship for project productivity

        # have project page info ready -


        # having project member page info ready

        # have a longitudinal data set ready
        # generate longtidinal dataset

        # generate IVs

        # generate CVs

        # merge IVs and DVs

    def select_valid_projects(self):
        # WikiProject Name Cleaning
        query = """
            SELECT title,
                   (FIRST(SPLIT(wikiproject, '1='))) AS wikiproject,
                   pageId,
                   importance,
                   class,
            FROM(SELECT title,
                       (FIRST(SPLIT(wikiproject, '{{'))) AS wikiproject,
                       pageId,
                       importance,
                       class,
                FROM (SELECT title,
                             (FIRST(SPLIT(wikiproject, '<!--'))) AS wikiproject,
                             pageId,
                             importance,
                             class,
                      FROM (SELECT title,
                                   (REPLACE(wikiproject, "#", "")) AS wikiproject,
                                   pageId,
                                   importance,
                                   class,
                            FROM (SELECT title,
                                          (REPLACE(wikiproject, "hawaii)", "hawaii")) AS wikiproject,
                                          pageId,
                                          importance,
                                          class,
                                   FROM (SELECT title,
                                               (REPLACE(wikiproject, "_", " ")) AS wikiproject,
                                               pageId,
                                               importance,
                                               class,
                                        FROM [bowen_user_dropouts.article_wikiproject])))))
            WHERE wikiproject <> 'requested articles' AND wikiproject <> 'articles for creation' AND wikiproject <> 'articles for creation (admin)' AND wikiproject <> 'deletion'
        """

    def create_project_pages(self):

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
                       SUM(amount) AS total_edits,
                       MIN(first_edit) AS first_edit
                FROM `{}.{}`
                GROUP BY user_text, user_id, wikiproject_page
        """.format(self.default_db, "project_pages_45")
        self.query.run_query(query, self.default_db, "dvar_user_wikiproject_coordination_edits")



def main():

    exe = Executor()
    exe.main()

main()