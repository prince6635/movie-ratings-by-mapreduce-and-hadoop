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

* How MapReduce scales / distributed computing:       
    ![Distributed](./assets/pics/mapreduce-distributed.png)

## Tools

* Python tool for big data: [Enthought canopy](https://www.enthought.com/)
    * mrjob package: for MapReduce
    Editor -> !pip install mrjob
* Sample data: http://grouplens.org/
    * datasets -> MovieLens 100K Dataset (ml-100k.zip)
