# BFS:
#     WHITE: the node has not been touched yet.
#     GRAY: just discovered this node and needs to explore it
#     BLACK: the node is fully explored
#
# Need to run process_marvel_data.py first to reorgnize the fields as:
#   Superhero_id|all related superhero IDs|distance|color
#
# command: !python superhero_relatons_by_BFS_iteration.py --target=100 ./target/BFS-iteration-step-0.txt > ./target/BFS-iteration-step-1.txt
# if no info shows for hero ID: 100, then keep running like,
# !python superhero_relatons_by_BFS_iteration.py --target=100 ./target/BFS-iteration-step-1.txt > ./target/BFS-iteration-step-2.txt
#
# output should be:
#     Counters from step 1:
#         Degrees of Separation:
#             Target ID 100 was hit with distance 2: 1
#     (means heroID=100 is only 1 degree separated from Hulk)
#
# To understand the code:
#   mapper iterates each line, reducer combines all the line with the same characterID again.
# mapper: only do one BFS iteration, it's not recursive, that's why have to keep running the command until hitting the result
# reducer: make sure to choose the shortest distance
#
# RawValueProtocol: avoid MapReduce job add/remove anything from the raw input file (sometimes it'll make it a json blob)

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class Node(object):
    """docstring for Node."""
    def __init__(self):
        self.characterID = ''
        self.connections = []
        self.distance = 9999
        self.color = 'WHITE'

    # FORMAT is: ID|EDGES|DISTANCE|COLOR
    def fromLine(self, line):
        fields = line.split('|')
        if (len(fields) == 4):
            self.characterID = fields[0]
            self.connections = fields[1].split(',')
            self.distance = int(fields[2])
            self.color = fields[3]

    def getLine(self):
        connections = ','.join(self.connections)
        return '|'.join((self.characterID, connections, str(self.distance), self.color))

class MRBFSIteration(MRJob):
    """docstring for MRBFSIteration."""

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(MRBFSIteration, self).configure_options()
        self.add_passthrough_option('--target', help="ID of character we are searching for")

    def mapper(self, _, line):
        node = Node()
        node.fromLine(line)

        # If this node needs to be expanded...
        if (node.color == 'GRAY'):
            for connection in node.connections:
                vnode = Node()
                vnode.characterID = connection
                vnode.distance = int(node.distance) + 1
                vnode.color = 'GRAY'

                if (self.options.target == connection):
                    counterName = ("Target ID " + connection + " was hit with distance " + str(vnode.distance))
                    self.increment_counter('Degree of Separation', counterName, 1)

                yield connection, vnode.getLine()

            # We've processed this node, so color it black
            node.color = 'BLACK'

        # Emit the input node so we don't lose it
        yield node.characterID, node.getLine()

    def reducer(self, key, values):
        edges = []
        distance = 9999
        color = 'WHITE'

        for value in values:
            node = Node()
            node.fromLine(value)

            if (len(node.connections) > 0):
                edges.extend(node.connections)

            if (node.distance < distance):
                distance = node.distance

            if (node.color == 'BLACK'):
                color = 'BLACK'

            if (node.color == 'GRAY' and color == 'WHITE'):
                color = 'GRAY'

        node = Node()
        node.characterID = key
        node.color = color
        node.connections = edges

        yield key, node.getLine()

if __name__ == '__main__':
    MRBFSIteration.run()
