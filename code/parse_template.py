from __future__ import print_function

from mw.xml_dump import Iterator
from mw.xml_dump import functions
import difflib


class DiffTemp:

    def __init__(self):
        self.empty_message = "**NONE**"

    def parse_file(self, input, output):
        dump = Iterator.from_file(functions.open_file(input))
        fout = open(output, "w")

        for page in dump:
            # ignore old version pages that were redirected
            if page.redirect:
                continue

            if page.namespace in [0, 1, 2, 3, 4, 5]:
                pre_template = []
                for rev in page:

                    # skip non registered editors
                    if rev.contributor.id is None:
                        continue

                    from time import mktime, strptime
                    pattern = '%Y%m%d%H%M%S'
                    epoch = int(mktime(strptime(str(rev.timestamp), pattern)))

                    import re
                    cur_template = re.findall(r'{{[^{{}}]*}}', rev.text)
                    added_template = [temp for temp in cur_template if temp not in pre_template]

                    #if len(added_template) == 0:
                    #    added_template = self.empty_message

                    record = {"rev_timestamp": epoch,"rev_id": rev.id,
                              "rev_user_text": rev.contributor.user_text,"rev_user_id": rev.contributor.id,
                              "rev_page_title": page.title, "rev_page_id": page.id,
                              "ns": page.namespace, "add_template": len(added_template)}
                    pre_template = cur_template

                    from json import dumps
                    print(dumps(record), file=fout)
            else:
                for rev in page:

                    # skip non registered editors
                    if rev.contributor.id is None:
                        continue

                    from time import mktime, strptime
                    pattern = '%Y%m%d%H%M%S'
                    epoch = int(mktime(strptime(str(rev.timestamp), pattern)))
                    record = {"rev_timestamp": epoch, "rev_id": rev.id,
                              "rev_user_text": rev.contributor.user_text, "rev_user_id": rev.contributor.id,
                              "rev_page_title": page.title, "rev_page_id": page.id,
                              "ns": page.namespace, "add_template": 0}
                    from json import dumps
                    print(dumps(record), file=fout)


def main(argv=None):
    if len(argv) != 3:
        print("usage: <input_file> <output_file>")
        return

    parser = DiffTemp()
    parser.parse_file(input=argv[1], output=argv[2])


if __name__ == '__main__':
    from sys import argv
    main(argv)
