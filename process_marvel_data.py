# Call this with one argument: the character ID you are starting from.
# For example, Spider Man is 5306, The Hulk is 2548. Refer to Marvel-names.txt
# for others.
# command: !python process_marvel_data.py 2548 (2548 is hulk)
# reorgnize the fields as: Superhero_id|all related superhero IDs|distance|color
# ex: 5988|748,1722,3752,4655,5743,1872,3413,5527,6368,6085,4319,4728,1636,2397,3364,4001,1614,1819,1585,732,2660,3952,2507,3891,2070,2239,2602,612,1352,5447,4548,1596,5488,1605,5517,11,479,2554,2043,17,865,4292,6312,473,534,1479,6375,4456|9999|WHITE

import sys

print 'Creating BFS starting input for character: ' + sys.argv[1]

with open("./target/BFS-iteration-step-0.txt", 'w') as out:

    with open("./assets/data/marvel/Marvel-graph.txt") as f:

        for line in f:
            fields = line.split()
            heroID = fields[0]
            numConnections = len(fields) - 1
            connections = fields[-numConnections:]

            color = 'WHITE'
            distance = 9999

            if (heroID == sys.argv[1]):
                color = 'GRAY'
                distance = 0

            if (heroID != ''):
                edges = ','.join(connections)
                outStr = '|'.join((heroID, edges, str(distance), color))
                out.write(outStr)
                out.write("\n")

    f.close()

out.close()
