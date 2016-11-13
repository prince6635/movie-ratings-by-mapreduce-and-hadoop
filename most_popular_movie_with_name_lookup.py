# add_file_option: the file in the parameter will be distributed along with the code of the script for every node that this job might run on (!!! otherwise, since MapReduce jobs are distributed in different instances/nodes, this file might not be available in some of them).
# _init is running before mapper/reducer
# command: !python most_popular_movie_with_name_lookup.py --items=./assets/data/ml-100k/u.item ./assets/data/ml-100k/u.data > ./target/results.txt

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):

    def configure_options(self):
        super(MostPopularMovie, self).configure_options()
        self.add_file_option('--items', help='Path to u.item that has movie names')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_passthrough,
                   reducer=self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_init(self):
        self.movieNames = {}

        with open("u.item") as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1].decode('utf-8', 'ignore')

    def reducer_count_ratings(self, movieID, counts):
        yield None, (sum(counts), self.movieNames[movieID])

    # This mapper does nothing; it's just here to avoid a bug in some
    # versions of mrjob related to "non-script steps." Normally this
    # wouldn't be needed.
    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
