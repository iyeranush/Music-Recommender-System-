# Anusha Radhakrishnan iyer
# 800872109
# musicrecommender.py
#
# Standalone Python/Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# TODO: Write this.
# 
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x.
#
# Usage: spark-submit musicrecommender.py <inputdatafile>
# Example usage: spark-submit musicrecommender.py aiyer5_train.csv
#
#

import sys
import numpy as np
from pyspark import SparkContext


if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: recommend <datafile>"
    exit(-1)
  sc = SparkContext(appName="RecommenderSystem")
  #Split Lines
  
  #####
  #train set
  train = sc.textFile(sys.argv[1])
  #Split train by white spaces
  trainA= train.map(lambda l:l.split())
  #create array of train
  trainB= np.array(trainA.collect(),dtype=object)
  ###The train set is in the following format
  #<UserID, songID, ArtistId, AlbumID, GenreID, Ratings_ByUser, Distance_setTo0>
  trainB=np.insert(trainB,[6],0,axis=1)

  ######
  #test set
  test = sc.textFile("practice/aiyer5_test.csv")
  #split test by white spaces
  testA= test.map(lambda l:l.split())
  #create array of test
  testB= np.array(testA.collect(), dtype=object)
  testB=np.insert(testB,[6],0,axis=1)
  ###The test set is in the following format
  #<UserID, songID, ArtistId, AlbumID, GenreID, Ratings_GroundTruth, Calculated_Rating>  

  
  #Create euclidean distance.
  #Write for loop to extract number of rows.
  #for test try all training

  ##### Distance using map
  # Have not used the function distance2. 
  def distance2(one , two):
    return np.sqrt(
        one.zip(two)
        .map(lambda x: (x[0]-x[1]))
        .map(lambda x: x*x)
        .reduce(lambda one,two: one+two)
        )
  # The distance measure for this project is the below function
  def levenshtein(str1,str2):
    # This function calculates the Levenshtein distance between two strings.
    x, y = len(str1), len(str2)
    if x > y:
        # Make sure n <= m, to use O(min(n,m)) space
        str1,str2 = str2,str1
        x,y = y,x
    #cur holds the value of mismatch     
    cur = range(x+1)
    for i in range(1,y+1):
    #check for similarity between the characters
        previous, cur = cur, [i]+[0]*x
        for j in range(1,x+1):
            add, rm = previous[j]+1, cur[j-1]+1
            change = previous[j-1]
            if str1[j-1] != str2[i-1]:
                change = change + 1
            cur[j] = min(add, rm, change)         
    return cur[x]
# comparing the train an test instances
  def traintestDistance(instance1, instance2, length):
      distance = 0
      for x in range(1,length):  
        distance +=levenshtein(instance1[x],instance2[x])    
      return distance 

  ###### To find train-test distance between the test querry
  ###### and all the tuples of train set
  
  for y in range(len(testB)):
    #accum stores the sum of ratings
    accum= sc.accumulator(0)
    for x in range(len(trainB)):
      distance = traintestDistance(trainB[x], testB[y], 4)
      #Taking the distance of 
      trainB[x,6]=distance
      #print 'Distance: ' + repr(distance)
    ##Sort the RDD
    ##SortBy the distance column
    ##Take 5 nearest Neighbour
    trainToSort=sc.parallelize(trainB).sortBy(lambda x: x[6]).take(7)
    sc.parallelize(trainToSort).foreach(lambda x: accum.add(int(x[5])))
    testB[y,6]=(accum.value)/7
     
  print 'This is test Data set with last two columns as <ground_Truth, KNN Result>:' 
  for y in range(len(testB)-1):
    print testB[y]  

  sc.stop()
