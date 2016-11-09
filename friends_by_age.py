# average number of friends for each age
# fields: user_id, name, age, # of friends
# command in Canopy (nagivate to the project root first): !python friends_by_age.py ./assets/data/fakefriends.csv > ./target/results.txt

from mrjob.job import MRJob

class MRFriendsByAge(MRJob):

    def mapper(self, _, line):
        (ID, name, age, numFriends) = line.split(',')
        yield age, float(numFriends)

    def reducer(self, age, numFriends):
        total = 0
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1

        yield age, total / numElements

if __name__ == '__main__':
    MRFriendsByAge.run()
