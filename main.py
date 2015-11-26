import csv
import json
import multiprocessing
import multiprocessing as mp
import operator
import threading as th
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing.dummy import Lock
import string
import re
import logging
import math
import msgpack

regex = re.compile('[%s]' % re.escape(string.punctuation))
out = regex.sub(' ', "This is, fortunately. A Test! string")


class TF_IDF:
    def __init__(self):
        self.words = {}
        self.tag_words = {}
        self.punctuation = ",.:?!'\"\\-"
        self.translation_table = (",.:?!'<>()\"", '           ')
        self.regex = re.compile('[%s]' % re.escape(self.punctuation))
        self.count_of_docs = 41353
        self.pool = ThreadPool(8)

    def split_data(self, tags, text):
        text = text.translate(self.translation_table).split()
        tags = tags.split()
        return tags, text

    def add_tagged_text(self, tags, text):

        # print(tags)
        # text = text.translate(self.translation_table).split()
        try:
            text = text.lower()
            text = regex.sub(' ', text).split()
            tags = tags.split()
        except IndexError:
            print('Error with string {}'.format(text))
            return

        for tag in tags:
            if tag not in self.tag_words:
                self.tag_words[tag] = {}
            temp = {}

            for word in text:
                if word in temp:
                    temp[word] += 1
                else:
                    temp[word] = 1

            for word in temp:
                if word in self.tag_words[tag]:
                    self.tag_words[tag][word] += temp[word]/len(text)
                else:
                    self.tag_words[tag][word] = temp[word]/len(text)

        for word in self.tag_words[tags[0]]:
            if word in self.words:
                self.words[word] += 1
            else:
                self.words[word] = 1

    def save_result(self):
        logging.info('Saving results')
        with open('tagged_words.json', 'w') as tw:
            json.dump(self.tag_words, tw)
        with open('words.json', 'w') as w:
            json.dump(self.words, w)
        logging.info('Finished')


    def load_results(self):
        self.tag_words = json.loads(open('tagged_words.json').read())
        self.words = json.loads(open('words.json').read())

        logging.info('Finished')

    def fast_sort(self):
        def sort(dictionary):
            self.tag_words[dictionary[0]] = \
                ({k, dictionary[1][k]} for k in sorted(dictionary[1], key=dictionary[1].get, reverse=True))
        self.pool.map(sort, self.tag_words.items())

    def calc(self):
        import pickle

        # def inner(dictionary):
        #     for word in dictionary[1]:
        #         self.tag_words[dictionary[0]][word] *= math.log10(self.count_of_docs/self.words[word])
        for dictionary in self.tag_words.items():
            for word in dictionary[1]:
                tmp = word.lower()
                try:
                    self.tag_words[dictionary[0]][tmp] *= math.log10(self.count_of_docs/self.words[tmp])
                except KeyError:
                    print('eeee')
        # self.pool.map(inner, self.tag_words.items())

        with open('matrix.pickle', 'wb') as m:
            pickle.dump(self.tag_words, m)


base = TF_IDF()

# print('----Processing csv----')
# with open('/home/vladimir/Train/TrainLight.csv', 'r') as train:
#             csv_reader = csv.reader(train, delimiter=',')
#             for row in csv_reader:
#                 base.add_tagged_text(row[3], row[2])
#
# # print('----Processing csv end!----')
# # print('----Save results----')
# base.save_result()
print('----Save results end!----')
print('----load results----')
base.load_results()
# # # print(len(base.tag_words))
# # # print('----load results end----')
# # # print('----calc----')
base.calc()
# print(len(base.tag_words))
# print('----calc end----')

text = 'How can I prevent firefox from closing when I press ctrl-w","<p>In my favorite editor (vim), I regularly use ctrl-w to execute a certain action. Now, it quite often happens to me that firefox is the active window (on windows) while I still look at vim (thinking vim is the active window) and press ctrl-w which closes firefox. This is not what I want. Is there a way to stop ctrl-w from closing firefox?'
punctuation = ",.:?!'\"\\-<>[]()"
regex = re.compile('[%s]' % re.escape(punctuation))
text = regex.sub(' ', text).split()

ranks = {}
def ranking(word):
    best_tag = ''
    best_value = 0
    for tag in base.tag_words:
        if word in base.tag_words[tag]:
            if base.tag_words[tag][word] > best_value:
                best_tag = tag
                best_value = base.tag_words[tag][word]
    if best_tag in ranks:
        ranks[best_tag] += best_value
    else:
        ranks[best_tag] = best_value

for word in text:
    ranking(word.lower())

pass



