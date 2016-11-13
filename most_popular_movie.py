# Find most rated movie in the entire data set
# ./assets/pics/mapreduce-example-most-rated-movie.png
# command: !python most_popular_movie.py ./assets/data/ml-100k/u.data > ./target/results.txt

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_passthrough,
                   reducer=self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, movieID, counts):
        yield None, (sum(counts), movieID)

    # This mapper does nothing; it's just here to avoid a bug in some
    # versions of mrjob related to "non-script steps." Normally this
    # wouldn't be needed.
    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
