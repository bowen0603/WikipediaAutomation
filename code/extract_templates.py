
import requests
import json
import numpy as np
# https://en.wikipedia.org/w/api.php?action=help&modules=query%2Bcategorymembers
# https://en.wikipedia.org/wiki/Special:ApiSandbox#action=query&format=json&list=categorymembers&cmtitle=Category%3AWikiProject+Military+history+templates&cmlimit=500


# https://en.wikipedia.org/wiki/Category:WikiProject_templates


# how to download files using python:
# import urllib
 #urllib.urlretrieve("http://google.com/index.html", filename="local/index.html")

 # os.system('wget http://somewebsite.net/shared/fshared_%s.7z'%i)

 # parallel programming: https://stackoverflow.com/questions/20548628/how-to-do-parallel-programming-in-python
 # full document: https://docs.python.org/2/library/multiprocessing.html
 # pool.map(parser, args)
#
# parser is the single program that process the diff, while args take in the parameters of the files

# TODO: page to start from: wikipedia.org

# dfs each page category to form: template name * template wikiproject * template category * path length

class TemplateExtractor:

    def __init__(self):

        self.nbr_random_sample = 200

        self.projects = []
        self.const_non_category = "NONE_CATE"
        self.template_query = "https://en.wikipedia.org/w/api.php?action=query&format=json&list=categorymembers&cmlimit=500" \
                    "&cmtitle={}"
        self.valid_wikiprojects = []

    def extract_all_template_wikiprojects(self):
        #"https://en.wikipedia.org/w/api.php?action=query&format=json&list=users"
        page_title = "Category:Wikipedia template categories"
        page_title = "Category:WikiProject templates"
        cnt_categories = 0
        fout = open('data/templates.csv', 'w')
        try:
            print("***First request***")
            query = "https://en.wikipedia.org/w/api.php?action=query&format=json&list=categorymembers&cmlimit=500" \
                    "&cmtitle={}".format(page_title)
            response = requests.get(query).json()
            for categorymember in response['query']['categorymembers']:
                pageid = categorymember['pageid']
                ns = categorymember['ns']
                title = categorymember['title']
                # print(title, file=fout)
                cnt_categories += 1

                if title.startswith('Category:WikiProject '):
                    self.projects.append(title)


            while 'cmcontinue' in response['continue']:
                print("***More requests***")
                cmcontinue = response['continue']['cmcontinue']
                query = "https://en.wikipedia.org/w/api.php?action=query&format=json&list=categorymembers&cmlimit=500" \
                        "&cmtitle={}&cmcontinue={}".format(page_title, cmcontinue)
                response = requests.get(query).json()
                for categorymember in response['query']['categorymembers']:
                    pageid = categorymember['pageid']
                    ns = categorymember['ns']
                    title = categorymember['title']
                    # print(title, file=fout)
                    cnt_categories += 1

                    if title.startswith('Category:WikiProject '):
                        self.projects.append(title)

        except Exception as e:
            print(e)

        print("{} categories in total. {} wikiproject specific templates.".format(cnt_categories, len(self.projects)))


    def read_valid_wikiprojects(self):
        fin = open("data/valid_wikiprojects.csv", "r")
        header = True
        for line in fin:
            if header:
                header = False
                continue
            wikiproject = line.split(",")[0]
            self.valid_wikiprojects.append(wikiproject)

    def search_project_templates(self):

        cnt = 0
        cnt_non_parents = 0
        cnt_total = 0
        fout = open("data/valid_project_templates.json", 'w')

        self.read_valid_wikiprojects()

        for template_project in self.projects:

            print("No. {}: {}".format(cnt, template_project))
            cnt += 1

            name_project = template_project.replace("Category:", "").replace(" templates", "").replace("WikiProject ", "").lower()
            if name_project not in self.valid_wikiprojects:
                continue

            # if name_project == 'WikiProject Journalism':
            #     pass

            templates = self.dfs_templates(name_project, self.const_non_category, template_project, [template_project], 0)

            for template in templates:

                title, parent_category, name_project, depth = template
                record = {"template": title, "top_category": parent_category,
                          "wikiproject": name_project, "depth": depth}
                from json import dumps
                print(dumps(record), file=fout)
                cnt_total += 1
                if depth == 0:
                    cnt_non_parents += 1
        print("Total Templates: {}; non-parent templates: {}".format(cnt_total, cnt_non_parents))


    def dfs_templates(self, name_project, parent_category, template_project, template_path, depth):

        if depth >= 8:
            return []

        templates = []
        try:
            query = self.template_query.format(template_project)
            response = requests.get(query).json()

            for categorymember in response['query']['categorymembers']:
                pageid = categorymember['pageid']
                ns = categorymember['ns']
                title = categorymember['title']
                # print(title, file=fout)

                # prevent duplications for infinite loops
                if title in template_path:
                    continue

                template_path.append(title)

                # subcategories
                if ns == 14:
                    # only keep the highest level of parent category
                    if depth == 0:
                        parent_category = title

                    sub_templates = self.dfs_templates(name_project, parent_category, title, template_path, depth+1)
                    templates += sub_templates

                # page
                if ns == 10:
                    # non sub category titles
                    templates.append((title, parent_category, name_project, depth))

                template_path.remove(title)

        except Exception as e:
            print(e)

        return templates

    def random_select_templates(self, depth):
        list_templates = []
        fin = open("data/valid_project_templates.json", "r")
        for line in fin:
            obj = json.loads(line.strip())
            if obj['depth'] > depth:
                continue
            list_templates.append(obj)

        random_templates = np.random.choice(list_templates, self.nbr_random_sample, replace=False)
        fout = open("data/random_project_templates_depth2.csv", "w")
        print("template,wikiproject,top_category,depth", file=fout)
        for template in random_templates:
            print("{},{},{},{}".format(template["template"],
                                       template["wikiproject"],
                                       template["top_category"],
                                       template["depth"]), file=fout)





def main():
    tx = TemplateExtractor()
    # tx.extract_all_template_wikiprojects()
    # tx.search_project_templates()
    tx.random_select_templates(2)

main()

#https://en.wikipedia.org/wiki/Special:ApiSandbox#action=query&format=json&list=categorymembers&cmtitle=Category%3AWikiProject+Military+history+templates&cmlimit=500