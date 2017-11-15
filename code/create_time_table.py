from __future__ import print_function

__author__ = 'bobo'

class Timer:

    def __init__(self):
        self.starting_time = 979697313
        self.ending_time = 1433343910
        self.time_period = 3600 * 24 * 7 # set a week for now
        self.fout = open("data/time_indices.csv", "w")

    def create_time_table(self):
        cur_time = self.starting_time
        idx = 0

        while True:
            if cur_time + self.time_period < self.ending_time:
                print("{},{},{}".format(cur_time, cur_time + self.time_period, idx), file=self.fout)
                idx += 1
                cur_time += self.time_period
            else:
                print("{},{},{}".format(cur_time, self.ending_time, idx), file=self.fout)
                break

def main():
    timer = Timer()
    timer.create_time_table()

main()