from __future__ import print_function

from random import randint
from time import sleep
#from multiprocessing import Pool
from multiprocessing.dummy import Pool
import os
#from urllib3 import urlretrieve
from urllib.request import urlretrieve

import urllib3

class ParallelProcessing:

    def __init__(self):
        self.thread_nbr = 8
        self.thread_download_nbr = 2

        self.url_base = "https://dumps.wikimedia.org/enwiki/20170701"
        self.dir_temporal = "data/temporal"

        self.urls = self.read_url_file()

    def parallel_processing_demo(self):
        lst = []
        cnt = 0
        for i in range(26): # total number of tasks

            if cnt < self.thread_nbr:
                cnt += 1
                length = randint(0, 10)
                lst.append((i, length))
            else:
                pool = Pool(self.thread_nbr)
                pool.map(self.foo, lst)
                print("All finished")

                cnt = 1
                length = randint(0, 10)
                lst = [(i, length)]
        if lst:
            pool = Pool(self.thread_nbr)
            pool.map(self.foo, lst)
            print("All finished")


    def foo(self, para):
        sleep(para[1])
        print("{}, Sept {} seconds.".format(para[0], para[1]))
        # return para[1]


    def read_url_file(self):
        urls = []
        for line in open('data/dump_urls.txt'):
            urls.append(line.strip())
        return urls

    def parallel_downloading(self):
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
            pool.map(self.process_dump, list_urls)

    def download_dump(self, params):

        idx, url = params[0], params[1]
        print("No. {} Processing - {}".format(idx, url))

        # download the dump
        url_full = "{}/{}".format(self.url_base, url)
        filename = "{}/{}".format(self.dir_temporal, url)

        urlretrieve(url_full, filename=filename)


    def parallel_processing(self):
        cnt = 0
        list_urls = []

        pool_downlowd = Pool(2)
        pool_worker = Pool(self.thread_nbr)

        pool = Pool(self.thread_nbr)
        for i in range(len(self.urls)):

            if cnt < self.thread_nbr:
                cnt += 1
                list_urls.append((i, self.urls[i]))
            else:
                # pool = Pool(self.thread_nbr)
                # pool.map_async(self.bash, list_urls)
                # pool.map(self.bash, list_urls)

                pool.map(self.process_dump, list_urls)
                cnt = 1
                list_urls = [(i, self.urls[i])]

        if list_urls:
            pool = Pool(self.thread_nbr)
            pool.map(self.process_dump, list_urls)
            pool.close()
            pool.join()


    def process_dump(self, params):

        idx = params[0]
        url = params[1]

        print("No. {} Processing - {}".format(idx, url))

        # download the dump
        url_full = "{}/{}".format(self.url_base, url)
        filename = "{}/{}".format(self.dir_temporal, url)


        print(url_full)
        urlretrieve(url_full, filename=filename)
        # command_wget = "wget -P {} -c '{}/{}'".format(self.dir_temporal, self.url_base, url)
        # print(command_wget)
        # os.system(command_wget)

        # process dump
        print("Parsing Dump No. {} ...".format(idx))

        # delete processed dump
        print("Deleting Dump No. {} ...".format(idx))
        os.remove(filename)
        # command_rm = "rm -f {}".format(filename)

        # os.system(command_rm)
        #rm -f "$TMPDIR${FILES[i]}"


def main():
    pp = ParallelProcessing()
    # pp.parallel_processing_demo()
    pp.parallel_downloading()

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