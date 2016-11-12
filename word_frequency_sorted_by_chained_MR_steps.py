# word-frequency-sorted-in-book-example.png
# Chanining steps: sort the first MapReduce results to return the results by count
# '%04d'%int(count) -> convert to integer and pad with 4 decimal points
# command: !python word_frequency_sorted_by_chained_MR_steps.py ./assets/data/book.txt > ./target/results.txt

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_make_counts_key,
                   reducer=self.reducer_output_words)
        ]

    def mapper_get_words(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            word = unicode(word, "utf-8", errors="ignore")
            yield word.lower(), 1

    def reducer_count_words(self, key, values):
        yield key, sum(values)

    def mapper_make_counts_key(self, word, count):
        yield '%04d'%int(count), word

    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word

if __name__ == '__main__':
    MRWordFrequencyCount.run()
