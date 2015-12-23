File name: musicrecommender.py

Standalone Python/Spark program to perform collaborative filtering using KNN.
Performs KNN using levenshtein distance between strings in train and test
TODO: Write this.

Takes the aiyer5_train.csv file as input, where on each line we have
<UserID, SongID, AlbumID, ArtistID, genreID, User_Ratings>
6 columns in total.
aiyer5_test.csv has
<UserID, SongID, AlbumID, ArtistID, genreID, Ground_Truth_User_Ratings>
6 columns in total

Usage: 
$ hadoop fs -put musicrecommender.py /user/<username>/
$ hadoop fs -put train.csv /user/<username>/
$ hadoop fs -put test.csv /user/<username>/
spark-submit musicrecommender.py <inputdatafile>
Example usage: spark-submit musicrecommender.py train.csv test.csv



