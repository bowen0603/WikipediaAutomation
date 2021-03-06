from __future__ import print_function

from random import randint
from time import sleep
from multiprocessing.dummy import Pool
import os
from urllib.request import urlretrieve
from parse_diff import DiffParser
from parse_template import DiffTemp

import urllib3

class ParallelProcessing:

    def __init__(self):
        self.thread_nbr = 3
        self.thread_download_nbr = 2

        self.url_base = "https://dumps.wikimedia.org/enwiki/20170701"
        self.dir_temporal = "/export/scratch2/bowen-yu/tempo_data"
        self.dir_output = "/export/scratch2/bowen-yu/parsed_dumps"
        self.dir_temporal_vm = "~/tempo_data"
        self.dir_output_vm = "~/parsed_dumps"

        self.file_bots = "data/bots_list.csv"

        self.urls = self.read_url_file()

    def read_url_file(self):
        urls = []
        for line in open('data/dump_urls.txt'):
            urls.append(line.strip())
        return urls

    def parallel_downloading(self):
        print("Downloading the dumps in two processes...")
        cnt = 0
        list_urls = []

        pool = Pool(self.thread_download_nbr)
        for i in range(len(self.urls)):

            if cnt < self.thread_download_nbr:
                cnt += 1
                list_urls.append((i, self.urls[i]))
            else:
                pool.map(self.download_dump, list_urls)
                cnt = 1
                list_urls = [(i, self.urls[i])]

        if list_urls:
            pool = Pool(self.thread_nbr)
            pool.map(self.download_dump, list_urls)

    def download_dump(self, params):

        idx, url = params[0], params[1]
        print("No. {} Processing - {}".format(idx, url))

        # download the dump
        url_full = "{}/{}".format(self.url_base, url)
        filename = "{}/{}".format(self.dir_temporal, url)

        urlretrieve(url_full, filename=filename)

    def read_filenames(self):
        from os import listdir
        from os.path import isfile, join
        path = self.dir_temporal + "/"
        return [f for f in listdir(path) if isfile(join(path, f))]

    def parallel_processing(self):
        cnt = 0
        list_dumpfile = []

        #filenames = self.read_filenames()
        # same name as urls, only for vm
        filenames = self.urls
        pool = Pool(self.thread_nbr)

        for i in range(len(filenames)):

            if cnt < self.thread_nbr:
                cnt += 1
                list_dumpfile.append((i, filenames[i]))
            else:
                #pool.map(self.process_dump, list_dumpfile)
                pool.map(self.process_dump_vm, list_dumpfile)
                cnt = 1
                list_dumpfile = [(i, filenames[i])]

        if list_dumpfile:
            pool = Pool(self.thread_nbr)
            #pool.map(self.process_dump, list_dumpfile)
            pool.map(self.process_dump_vm, list_dumpfile)
            pool.close()
            pool.join()


    def process_dump(self, params):

        idx = params[0]
        dumpfile = params[1]

        # process dump
        print("Parsing Dump No. {}: {} ...".format(idx, dumpfile))

        input = "{}/{}".format(self.dir_temporal, dumpfile)
        output = "{}/{}".format(self.dir_output, dumpfile).replace(".7z", ".json")
        bot_file = "{}/{}".format(os.getcwd(), self.file_bots)

        #parser = DiffParser()
        parser = DiffTemp()
        parser.parse_file(input, output)

        # delete processed dump
        # print("Deleting Dump No. {} ...".format(idx))

    def process_dump_vm(self, params):
        idx = params[0]
        dumpfile = params[1]

        # process dump
        print("Parsing Dump No. {}: {} ...".format(idx, dumpfile))
        input = "{}/{}".format(self.dir_temporal_vm, dumpfile)
        output = "{}/{}".format(self.dir_output_vm, dumpfile).replace(".7z", ".json")
        bot_file = "{}/{}".format(os.getcwd(), self.file_bots)

        print(input, output)

        # first scp the file from barcelona
        command = "sshpass -p \"ybw220///\" scp bowen-yu@flagon.cs.umn.edu:/export/scratch2/bowen-yu/dumpfile input"
        command = "sshpass -p \"ybw220///\" scp bowen-yu@flagon.cs.umn.edu:/export/scratch2/bowen-yu/tempo_data/{} {}".format(dumpfile, input)
        print(command)
        os.system(command)

        # sleep for 10+ seconds until the file is completed
        import time
        time.sleep(10)

        # parse the file
        #parser = DiffTemp()
        #parser.parse_file(input, output)
        # delete processed dump
        # print("Deleting Dump No. {} ...".format(idx))

        # scp the parsed file to barcelona
        command = "sshpass -p \"ybw220///\" scp output bowen-yu@flagon.cs.umn.edu:/export/scratch2/bowen-yu/vm_parsed"
        print(command)
        os.system(command)

        # delete files
        #os.system("rm -f input")
        #os.system("rm -f output")
        time.sleep(10)



def main():
    pp = ParallelProcessing()
    # pp.parallel_processing_demo()
    # pp.parallel_downloading()
    pp.parallel_processing()

main()


# how to download files using python:
# import urllib
 #urllib.urlretrieve("http://google.com/index.html", filename="local/index.html")

 # os.system('wget http://somewebsite.net/shared/fshared_%s.7z'%i)

 # parallel programming: https://stackoverflow.com/questions/20548628/how-to-do-parallel-programming-in-python
 # full document: https://docs.python.org/2/library/multiprocessing.html
 # pool.map(parser, args)
#
# parser is the single program that process the diff, while args take in the parameters of the files

# https://stackoverflow.com/questions/21276672/download-files-from-url-parallely-in-python
