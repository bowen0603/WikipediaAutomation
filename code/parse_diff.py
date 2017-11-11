from __future__ import print_function

from mw.xml_dump import Iterator
from mw.xml_dump import functions
import difflib
###############################################################
## Code Description
## Steps of diff operation(function "parse_file")
#  1. split the whole revision content to sentenses in function "useful_text", and leave out the sentences without letters in function "is_useful"
#  2. in one revision of one page, "revtext" is the set of all the sentenses of last revision text, 
#  "current_revtext" is the set of all the sentenses of the current revision text
#  3. in function "diff_sentenses", compare "revtext" and "current_revtext", and output the diff result
#  4. the function "diff_text" is used to compare two content by letters not sentenses, it is used in the old diff operation not in the current one.
## the code take in one meta dump page current file as input for parsing.
## it parses all the revision record in the file ignoring those edited by bots, and anonymous editors.
## it parses handles all the namespaces, but will not extract the particular editing texts.
##
###############################################################
## Revision Records
## for each revision record, mark down all its importance information. timestamp is converted to epoch
## schema: {"rev_id": rev.id, "rev_page_id": page.id, "rev_page_title": page.title,
##                      "rev_user_id": rev.contributor.id, "rev_user_text": rev.contributor.user_text,
##                      "ns": page.namespace, "rev_timestamp": epoch}
##
###############################################################
## Input files: <input_file> <output_file> <bot_file>
## dump meta current page file; output file name with file type; bot list file
##
## Choose whether to store the categories or wikiprojects of an article in a list,
## or single record per line by setting LIST_FORMAT
##

class DiffParser:
    def load_bots(self, bot_file):
        bot_list = []
        for bot in open(bot_file, 'r'):
            bot_list.append(bot)
        return bot_list

    def diff_text(self, a,b):
        diff_result = ""
        tempstr = ''
        operator = 0
        for i, s in enumerate(difflib.ndiff(a, b)):
            if s[0] == ' ':
                if operator == -1:
                    #print(u'Delete:\n "{}"'.format(tempstr))
                    diff_result += '*Delete:* {}  '.format(tempstr)
                    tempstr = ''
                    operator = 0
                elif operator == 1:
                    #print(u'Add:\n "{}"'.format(tempstr))
                    diff_result += '*Add:* {}  '.format(tempstr)
                    tempstr = ''
                    operator = 0
                else:
                    operator = 0

            elif s[0] == '-':
                if operator == 1:
                    #print(u'Add:\n "{}"'.format(tempstr))
                    diff_result += '*Add:* {}  '.format(tempstr)
                    tempstr = ''
                    tempstr += s[-1]
                    #print(u'Delete:\n "{}"'.format(tempstr))
                    diff_result += '*Delete:* {}  '.format(tempstr)
                else:
                    tempstr += s[-1]
                    operator = -1
            elif s[0] == '+':
                if operator == -1:
                    #print(u'Delete:\n "{}"'.format(tempstr))
                    diff_result += '*Delete:* {}  '.format(tempstr)
                    tempstr = ''
                    tempstr += s[-1]
                    #print(u'Add:\n "{}"'.format(tempstr))
                    diff_result += '*Add:* {}  '.format(tempstr)
                else:
                    tempstr += s[-1]
                    operator = 1
        if operator == -1:
            #print(u'Delete:\n "{}"'.format(tempstr))
            diff_result += '*Delete:* {}  '.format(tempstr)
            tempstr = ''
            operator = 0
        elif operator == 1:
            #print(u'Add:\n "{}"'.format(tempstr))
            diff_result += '*Add:* {}  '.format(tempstr)
            tempstr = ''
            operator = 0
        else:
            operator = 0
        return diff_result

    def is_useful(self, str):
        #set = ['a','b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', ]
        for word in str:
            if word.isalpha() == True:
                return 1
        return 0

    #diff operation
    def diff_sentences(self, s1,s2):
        diff_result = ""
        # in the first loop, if a sentence in s1 can't be found in s2, then output "delete"
        #for i in s1:
        #    flag = 0
        #    for j in s2:
        #        if(i == j):
        #            flag = 1
        #            break
        #    if(flag == 0):
        #        diff_result += '*Delete:* {}  '.format(i)
        # in the second loop, if a sentence in s2 can't be found in s1, then output "add"
        for i in s2:
            flag = 0
            for j in s1:
                if(i == j):
                    flag = 1
                    break
            if(flag == 0):
                diff_result += '*Add:* {}  '.format(i)
        return diff_result

    def useful_text(self, text):
        result = []
        sentence = text.splitlines()
        for sen in sentence:
            if (self.is_useful(sen) == 1):
                result.append(sen)
        return result

    #main function
    def parse_file(self, input=None, output=None, bot_file=None):
        bot_list = self.load_bots(bot_file)

        fout = open(output, 'w')
        dump = Iterator.from_file(functions.open_file(input))

        for page in dump:
            # ignore old version pages that were redirected
            if page.redirect:
                continue
            diff_content = ""

            if page.namespace in [3, 5]:
                #print("{},{}".format(page.title, page.namespace))
                #"revtext" is the last revision text, initialized as null
                revtext = []
                for rev in page:
                    #if rev.contributor.user_text in bot_list:
                    #    continue

                    from time import mktime, strptime
                    pattern = '%Y%m%d%H%M%S'
                    epoch = int(mktime(strptime(str(rev.timestamp), pattern)))
                    #"current_revtext" is the current revision text(a set of sentences)
                    current_revtext = self.useful_text(rev.text)
                    #diff operation
                    diff_content = self.diff_sentences(revtext,current_revtext)
                    record = {"rev_timestamp": epoch,"rev_id": rev.id,
                              "rev_user_text": rev.contributor.user_text,"rev_user_id": rev.contributor.id,
                              "rev_page_title": page.title,"rev_page_id": page.id,
                              "ns": page.namespace,"rev_diff": diff_content}
                    revtext = current_revtext
                    from json import dumps
                    print(dumps(record), file=fout)
            else:
                for rev in page:
                    diff_content = "None"
                    from time import mktime, strptime
                    pattern = '%Y%m%d%H%M%S'
                    epoch = int(mktime(strptime(str(rev.timestamp), pattern)))
                    record = {"rev_timestamp": epoch,"rev_id": rev.id,
                              "rev_user_text": rev.contributor.user_text,"rev_user_id": rev.contributor.id,
                              "rev_page_title": page.title,"rev_page_id": page.id,
                              "ns": page.namespace,"rev_diff": diff_content}
                    from json import dumps
                    print(dumps(record), file=fout)


def main(argv=None):
    # give the input and output filenames, wp and cat data will be parsed into different folders
    if len(argv) != 4:
        print("usage: <input_file> <output_file> <bot_file>")
        return

    parser = DiffParser()
    parser.parse_file(input=argv[1], output=argv[2], bot_file=argv[3])

if __name__ == '__main__':
    from sys import argv
    main(argv)
