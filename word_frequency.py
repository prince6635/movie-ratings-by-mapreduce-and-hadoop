# count of each word in book.txt
# .split just simply spit the words by empty space, tab, etc.
# command: !python word_frequency.py ./assets/data/book.txt > ./target/results.txt

from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word = unicode(word, "utf-8", errors="ignore") # avoid issues in mrjob 5.0
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()
