from __future__ import print_function

from mw.xml_dump import Iterator
from mw.xml_dump import functions
import difflib
###############################################################
## Code Description
##
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

def load_bots(bot_file):
    bot_list = []
    for bot in open(bot_file, 'r'):
        bot_list.append(bot)
    return bot_list

def diff_text(a,b):
    diff_result = ""
    tempstr = ''
    operator = 0
    for i, s in enumerate(difflib.ndiff(a, b)):
        if s[0] == ' ':
            if operator == -1:
                #print(u'Delete:\n "{}"'.format(tempstr))
                diff_result += 'Delete: {}  '.format(tempstr)
                tempstr = ''
                operator = 0
            elif operator == 1:
                #print(u'Add:\n "{}"'.format(tempstr))
                diff_result += 'Add: {}  '.format(tempstr)
                tempstr = ''
                operator = 0
            else:
                operator = 0

        elif s[0] == '-':
            if operator == 1:
                #print(u'Add:\n "{}"'.format(tempstr))
                diff_result += 'Add: {}  '.format(tempstr)
                tempstr = ''
                tempstr += s[-1]
                #print(u'Delete:\n "{}"'.format(tempstr))
                diff_result += 'Delete: {}  '.format(tempstr)
            else:
                tempstr += s[-1]
                operator = -1
        elif s[0] == '+':
            if operator == -1:
                #print(u'Delete:\n "{}"'.format(tempstr))
                diff_result += 'Delete: {}  '.format(tempstr)
                tempstr = ''
                tempstr += s[-1]
                #print(u'Add:\n "{}"'.format(tempstr))
                diff_result += 'Add: {}  '.format(tempstr)
            else:
                tempstr += s[-1]
                operator = 1
    if operator == -1:
        #print(u'Delete:\n "{}"'.format(tempstr))
        diff_result += 'Delete: {}  '.format(tempstr)
        tempstr = ''
        operator = 0
    elif operator == 1:
        #print(u'Add:\n "{}"'.format(tempstr))
        diff_result += 'Add: {}  '.format(tempstr)
        tempstr = ''
        operator = 0
    else:
        operator = 0
    return diff_result

def is_useful(str):
    #set = ['a','b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', ]
    for word in str:
        if word.isalpha() == True:
            return 1
    return 0

def diff_sentences(s1,s2):
    diff_result = ""
    for i in s1:
        flag = 0
        for j in s2:
            if(i == j):
                flag = 1
                break
        if(flag == 0):
            diff_result += 'Delete: {}  '.format(i)
    for i in s2:
        flag = 0
        for j in s1:
            if(i == j):
                flag = 1
                break
        if(flag == 0):
            diff_result += 'Add: {}  '.format(i)
    return diff_result

def useful_text(text):
    result = []
    sentence = text.splitlines()
    for sen in sentence:
        if (is_useful(sen) == 1):
            result.append(sen)
    return result

def parse_file(input=None, output=None, bot_file=None):

    #bot_list = load_bots(bot_file)

    fout = open(output, 'w')
    dump = Iterator.from_file(functions.open_file(input))

    for page in dump:
        # ignore old version pages that were redirected
        if page.redirect:
            continue
        diff_content = ""

        if page.namespace in [1, 3, 5]:
            print("{},{}".format(page.title, page.namespace))
            revtext = []
            for rev in page:
                from time import mktime, strptime
                pattern = '%Y%m%d%H%M%S'
                epoch = int(mktime(strptime(str(rev.timestamp), pattern)))
                current_revtext = useful_text(rev.text)
                diff_content = diff_sentences(revtext,current_revtext)
                record = {"rev_timestamp": epoch,"rev_id": rev.id,"rev_user_text": rev.contributor.user_text,"rev_user_id": rev.contributor.id,"rev_page_title": page.title,"rev_page_id": page.id,"ns": page.namespace,"rev_diff": diff_content}
                revtext = current_revtext
                from json import dumps
                print(dumps(record), file=fout)
        else:
            for rev in page:
                diff_content = "None"
                from time import mktime, strptime
                pattern = '%Y%m%d%H%M%S'
                epoch = int(mktime(strptime(str(rev.timestamp), pattern)))
                record  = {"rev_timestamp": epoch,"rev_id": rev.id,"rev_user_text": rev.contributor.user_text,"rev_user_id": rev.contributor.id,"rev_page_title": page.title,"rev_page_id": page.id,"ns": page.namespace,"rev_diff": diff_content}
                from json import dumps
                print(dumps(record), file=fout)

    return

def main(argv=None):
    # give the input and output filenames, wp and cat data will be parsed into different folders
    if len(argv) != 4:
        print("usage: <input_file> <output_file> <bot_file>")
        return

    parse_file(input=argv[1], output=argv[2], bot_file=argv[3])

if __name__ == '__main__':
    from sys import argv
    main(argv)
