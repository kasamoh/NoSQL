# NoSQL
NoSQL projects 


### First project  : 
#### Flask app for movies website : Flask , HTML , MongoDB , Python ####

![GitHub Logo](https://github.com/kasamoh/NoSQL/blob/master/mflix.png)






# Second Project 
<a style="color: black" href="http://andreiarion.github.io/Projet2018-intro.html#/">projet</a>
## But

Design a high-performance distributed storage system on AWS that can analyze the events of the year 2018 through their story in the world media collected by GDELT. The goal is to analyze trends and relationships between different country actors.
* We used spark as an ETL , with its native language Scala: we created a first script that loads the data (700 GB) in S3 , format parquet, and a second script scala to do the intermediate data processing and deposit the cleaned data on MongoDB & Cassandra instances deployed on AWS.
* We request the databases from python with the appropriate connectors of each base (with pymongo for MongoDB for example)
* We used Flask for the visualization part

Keywords: AWS EC2, ZooKeeper, S3, Zepplin, scala, mongoDB, Cassandra, Flask, Python
## Architecture

![Architcture](https://raw.githubusercontent.com/rreinette/INF728/master/Img/Screenshot%20from%202019-01-28%2009-42-38.png)
