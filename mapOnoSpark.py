# Spark example to print the average tweet length using Spark
# PGT April 2016   
# To run, do: spark-submit --master yarn-client avgTweetLength.py hdfs://hadoop2-0-0/data/twitter/part-03212

from __future__ import print_function
import sys, json
from pyspark import SparkContext
import datetime
# Given a full tweet object, return the text of the tweet
dateList =[]
def getText(line):
  try:
    input_dict = json.loads(line)
    user_dict = input_dict['user']
    if user_dict['screen_name'] == 'PrezOno':
      date = input_dict['created_at']
      dt = datetime.datetime.strptime(date,'%a %b %d %H:%M:%S +0000 %Y')
      datetest = dt.strftime('%b%d %H')
     # datetest1 = dt.strftime('%H')
      return [datetest]
    #if not(datetest in dateList):
     # dateList.append(datetest)
      
    else:
      return[]
      
  except Exception as a:
    return[]
  
    
  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="Avg Tweet time")
  
  tweets = sc.textFile(sys.argv[1],)
  
  time1 = tweets.flatMap(getText)
  tot = time1.map(lambda a: a.split(" ")[0])
  hours_arr = time1.map(lambda a: a.split(" ")[1])
  hours = hours_arr.map(lambda a: (a,1)).reduceByKey(lambda a,b: a+b)
  #totaldays = tot.flatMap(lambda a: a.split(" ")).map(lambda a: (a,1)).reduceByKey(lambda a,b: a+b)
  totaldays = tot.map(lambda a: (a,1)).reduceByKey(lambda a,b: a+b)
 
  no_of_days = totaldays.count()
  #avgTweetPerHour = hours.map(lambda a:a[1]/no_of_days)
  if not (no_of_days == 0):
    hoursAvg = hours.mapValues(lambda a: float(a)/no_of_days)
    averageHours = hoursAvg.collect()
    # hours.values
  
  # Save to your local HDFS folder
  
  hoursAvg.coalesce(1).saveAsTextFile("Avg hours")
  
  
  sc.stop()

