# Find similar movies with MapReduce to recommend
#
# find every movie that's watched together by the same people and
# measure how similar those movies are based on how people have rated them
#
# final output: movie sorted by name -> all other movies that's been watched with this movie and sorted by the strength of their similarity scores.

# commands:
# to run locally:
# !python ./movie_recommendation_by_similarity.py --items=./assets/data/ml-100k/u.item ./assets/data/ml-100k/u.data > ./target/sims.txt
#
# to run on a single EMR node:
# !python ./movie_recommendation_by_similarity.py -r emr --items=./assets/data/ml-100k/u.item ./assets/data/ml-100k/u.data
#
# to run on 4 EMR nodes:
# !python ./movie_recommendation_by_similarity.py -r emr --num-ec2-instances=4 --items=./assets/data/ml-100k/u.item ./assets/data/ml-100k/u.data
#
# troubleshooting EMR jobs (substitute your job ID):
# !python -m mrjob.tools.emr.fetch_logs --find-failure j-1NXMMBNEQHAFT

from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from itertools import combinations

class MovieRecommendationBySimilarity(MRJob):

    def configure_options(self):
        super(MovieRecommendationBySimilarity, self).configure_options()
        self.add_file_option('--items', help="Path to u.item")

    def load_movie_names(self):
        # Load database of movie names
        self.movieNames = {}

        with open("u.item") as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[int(fields[0])] = fields[1].decode('utf-8', 'ignore')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                   reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                   reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                   mapper_init=self.load_movie_names,
                   reducer=self.reducer_output_similarities)
        ]

    def mapper_parse_input(self, key, line):
        # Output userID => (movieID, rating)
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield userID, (movieID, float(rating))

    def reducer_ratings_by_user(self, user_id, itemRatings):
        # Group (item, rating) pairs by userID

        ratings = []
        for movieID, rating in itemRatings:
            ratings.append((movieID, rating))

        yield user_id, ratings

    def mapper_create_item_pairs(self, user_id, itemRatings):
        # Find every pair of movies each user has seen,
        # and emit each pair with its associated ratings

        # "combinations" finds every possible pair from the list of movies this user viewed
        for itemRating1, itemRating2 in combinations(itemRatings, 2):
            movieID1 = itemRating1[0]
            rating1 = itemRating1[1]
            movieID2 = itemRating2[0]
            rating2 = itemRating2[1]

            # Produce both others so sims are bi-directional
            yield (movieID1, movieID2), (rating1, rating2)
            yield (movieID2, movieID1), (rating2, rating1)

    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two rating vectors

        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)

    def reducer_compute_similarity(self, moviePair, ratingPairs):
        # Compute the similarity score between the ratings vectors
        # for each movie pair viewed by multiple people

        # Output movie pair => score, number of co-ratings
        score, numPairs = self.cosine_similarity(ratingPairs)

        # Enforce a minimum score and minimum number of co-ratings to ensure quality
        if (numPairs > 10 and score > 0.95):
            yield moviePair, (score, numPairs)

    def mapper_sort_similarities(self, moviePair, scores):
        # Shuffle things around so the key is (movie1, score)
        # so we have meaningfully sorted results.

        score, numPairs = scores
        movie1, movie2 = moviePair

        yield (self.movieNames[int(movie1)], score), (self.movieNames[int(movie2)], numPairs)

    def reducer_output_similarities(self, movieScore, similarN):
        # Output the results.
        # Movie => Simiar Movie, score, number of co-ratings
        movie1, score = movieScore
        for movie2, numPairs in similarN:
            yield movie1, (movie2, score, numPairs)

if __name__ == '__main__':
    MovieRecommendationBySimilarity.run()
