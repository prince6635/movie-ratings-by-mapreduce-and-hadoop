# count of each word in book.txt
# use regex to split the words
# command: !python word_frequency_better.py ./assets/data/book.txt > ./target/results.txt

from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            word = unicode(word, "utf-8", errors="ignore")
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()
