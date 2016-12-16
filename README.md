# Big data (movie ratings) based on Hadoop and MapReduce

## MapReduce
![](./assets/pics/mapreduce.png)
* Exmaple:
    * how many movies that each user has watched? => key: user_id and value: movie_id, now duplicate keys are ok, since reducer will handle that later.
    ![Sample data](./assets/pics/mapreduce-example-1.png)

    Map:
    ![Map I](./assets/pics/mapreduce-example-1.png)

    ![Map II](./assets/pics/mapreduce-example-2.png)

    ![Map III](./assets/pics/mapreduce-example-3.png)

    Reduce:
    ![Reduce](./assets/pics/mapreduce-example-4.png)

    All:
    ![All](./assets/pics/mapreduce-example-5.png)

    * Code snippet: # of movies for each rating?
        * Fields: user_id movie_id rating timestamp
        ![All](./assets/pics/mapreduce-example-6.png)

* Combiner: when mapper is done producing key-value pairs, do some reduction work in mapper, like aggregating data before sending to reducer to save some network bandwidth.
    - ex: ./word_frequency_with_combiner.py

* Attach config/data file with each MapReduce job across distributed nodes: ./most_popular_movie_with_name_lookup.py

* How MapReduce scales / distributed computing:       
    ![Distributed](./assets/pics/mapreduce-distributed.png)

## Hadoop (Run MapReduce job in a distributed way)
![Hadoop](./assets/pics/hadoop.png)

* HDFS (Hadoop Distributed File System): is used by Hadoop for distributing data and information that Hadoop accesses, YARN manages how Hadoop jobs distributed across the cluster.
![HDFS](./assets/pics/hadoop-HDFS.png)

* Apache YARN (Hadoop uses to figure out what mapper/reducer to run where, how to connect them all together, keep tracking what's running, etc.)
![YARN](./assets/pics/hadoop-YARN.png)

* AWS Elastic MapReduce
![EMR](./assets/pics/hadoop-AWS_EMR.png)

## Tools

* Python tool for big data: [Enthought canopy](https://www.enthought.com/)
    * mrjob package: for MapReduce
    Editor -> !pip install mrjob
* Sample data: http://grouplens.org/
    * datasets -> MovieLens 100K Dataset (ml-100k.zip)
