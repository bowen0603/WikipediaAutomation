__author__ = 'bobo'

class Analyser:
    def __init__(self):
        self.sig_thr = 0.1
        self.bot_list = {}

        self.read_bot_list()

    def read_bot_list(self):
        header = True
        for line in open("data/bot_id_list.csv", "r"):
            if header:
                header = False
                continue

            nbot = str(line.split(",")[0])
            bot_name = str(line.split(",")[1].strip())
            self.bot_list[nbot] = bot_name

    def read_sig_bots(self, filename):
        bot_coef = {}
        bot_pval = {}

        header = True
        for line in open(filename, "r"):
            if header:
                header = False
                continue

            nbot = str(line.split("\t")[1].strip())
            coef = float(line.split("\t")[2].strip())
            if coef == 0:
                continue

            if line.split("\t")[3].strip() == '.':
                continue

            pval = float(line.split("\t")[3].strip())
            if pval > self.sig_thr:
                continue

            bot_coef[nbot] = coef
            bot_pval[nbot] = pval

        return bot_coef, bot_pval

    # bots whose edits increase human productivity
    def find_good_bots_on_prod_human(self):
        bot_coef, bot_pval = self.read_sig_bots("data/single_bot/DV_prod_human_IV01.csv")
        import operator
        bot_coef = sorted(bot_coef.items(), key=operator.itemgetter(1), reverse=True)
        print("bot, coef, pval")
        for nbot, coef in bot_coef:
            print("{},{},{}".format(self.bot_list[nbot],
                                    coef,
                                    bot_pval[nbot]))
        print("Done with bots increasing article productivity by human")

    # bots whose edits increase article quality
    def find_good_bots_on_quality(self):
        bot_coef, bot_pval = self.read_sig_bots("data/single_bot/DV_quality_IV01.csv")
        import operator
        bot_coef = sorted(bot_coef.items(), key=operator.itemgetter(1), reverse=True)
        print("bot, coef, pval")
        for nbot, coef in bot_coef:
            print("{},{},{}".format(self.bot_list[nbot],
                                    coef,
                                    bot_pval[nbot]))
        print("Done with bots increasing article quality")

def main():
    ana = Analyser()
    ana.find_good_bots_on_prod_human()
    ana.find_good_bots_on_quality()

    # todo: bots that increase both quality and productivity?

    # todo: bots edits on other namespaces?

    # todo: recheck the computation process..


main()