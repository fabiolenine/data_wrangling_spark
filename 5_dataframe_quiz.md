
# Data Wrangling with DataFrames Coding Quiz

Use this Jupyter notebook to find the answers to the quiz in the previous section. There is an answer key in the next part of the lesson.


```python
# 1) import any other libraries you might need
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as Fsum
import pyspark.sql.functions as F

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 
```


```python
# 2) instantiate a Spark session
spark = SparkSession.builder \
        .appName("DataFrames Practice") \
        .getOrCreate()
    
# .builder - A Class attribute having a Builder to construct SparkSession instances.
# .appName("DataFrames Practice") - Sets a name for the application, which will be shown in the Spark web UI
#                                   If no application name is set, a randomly generated name will be used.
# .getOrCreate() - Gets an existing SparkSession or, it there is no existing one, creates a new one based on 
#                  options set in this builder.
```


```python
# 3) read in the data set located at the path "data/sparkify_log_small.json"
df = spark.read.json("data/sparkify_log_small.json")
```

# Question 1

Which page did user id "" (empty string) NOT visit?


```python
df.printSchema()
```

    root
     |-- artist: string (nullable = true)
     |-- auth: string (nullable = true)
     |-- firstName: string (nullable = true)
     |-- gender: string (nullable = true)
     |-- itemInSession: long (nullable = true)
     |-- lastName: string (nullable = true)
     |-- length: double (nullable = true)
     |-- level: string (nullable = true)
     |-- location: string (nullable = true)
     |-- method: string (nullable = true)
     |-- page: string (nullable = true)
     |-- registration: long (nullable = true)
     |-- sessionId: long (nullable = true)
     |-- song: string (nullable = true)
     |-- status: long (nullable = true)
     |-- ts: long (nullable = true)
     |-- userAgent: string (nullable = true)
     |-- userId: string (nullable = true)
    



```python
# TODO: write your code to answer question 1

# filter for users with blank user id
blank_pages = df.filter(df.userId == '') \
    .select(col('page') \
    .alias('blank_pages')) \
    .dropDuplicates()

# get a list of possible pages that could be visited
all_pages = df.select('page').dropDuplicates()

# find values in all_pages that are not in blank_pages
# these are the pages that the blank user did not go to
for row in set(all_pages.collect()) - set(blank_pages.collect()):
    print(row.page)
```

    Submit Upgrade
    Error
    NextSong
    Settings
    Logout
    Save Settings
    Submit Downgrade
    Upgrade
    Downgrade


# Question 2 - Reflect

What type of user does the empty string user id most likely refer to?



```python
# TODO: use this space to explore the behavior of the user with an empty string

#Perhaps it represents users who have not signed up yet or who are signed out and are about to log in.
```

# Question 3

How many female users do we have in the data set?


```python
# TODO: write your code to answer question 3
df.filter(df.gender == 'F') \
    .select('userId', 'gender') \
    .dropDuplicates() \
    .count()
```




    462



# Question 4

How many songs were played from the most played artist?


```python
all_pages.show()
```

    +----------------+
    |            page|
    +----------------+
    |Submit Downgrade|
    |            Home|
    |       Downgrade|
    |          Logout|
    |   Save Settings|
    |           About|
    |        Settings|
    |           Login|
    |        NextSong|
    |            Help|
    |         Upgrade|
    |           Error|
    |  Submit Upgrade|
    +----------------+
    



```python
# TODO: write your code to answer question 4
df.filter(df.page == 'NextSong') \
    .select('Artist') \
    .groupBy('Artist') \
    .agg({'Artist':'count'}) \
    .withColumnRenamed('count(Artist)', 'Artistcount') \
    .sort(desc('Artistcount')) \
    .show(1)
```

    +--------+-----------+
    |  Artist|Artistcount|
    +--------+-----------+
    |Coldplay|         83|
    +--------+-----------+
    only showing top 1 row
    


# Question 5 (challenge)

How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.




```python
# TODO: write your code to answer question 5
function = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

user_window = Window.partitionBy('userId').orderBy(desc('ts')).rangeBetween(Window.unboundedPreceding, 0)

cusum = df.filter((df.page == 'NextSong') | (df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', function(col('page'))) \
    .withColumn('period', Fsum('homevisit').over(user_window))
```


```python
cusum.show()
```

    +------+--------+-------------+---------+------+
    |userID|    page|           ts|homevisit|period|
    +------+--------+-------------+---------+------+
    |  1436|NextSong|1513783259284|        0|     0|
    |  1436|NextSong|1513782858284|        0|     0|
    |  2088|    Home|1513805972284|        1|     1|
    |  2088|NextSong|1513805859284|        0|     1|
    |  2088|NextSong|1513805494284|        0|     1|
    |  2088|NextSong|1513805065284|        0|     1|
    |  2088|NextSong|1513804786284|        0|     1|
    |  2088|NextSong|1513804555284|        0|     1|
    |  2088|NextSong|1513804196284|        0|     1|
    |  2088|NextSong|1513803967284|        0|     1|
    |  2088|NextSong|1513803820284|        0|     1|
    |  2088|NextSong|1513803651284|        0|     1|
    |  2088|NextSong|1513803413284|        0|     1|
    |  2088|NextSong|1513803254284|        0|     1|
    |  2088|NextSong|1513803057284|        0|     1|
    |  2088|NextSong|1513802824284|        0|     1|
    |  2162|NextSong|1513781246284|        0|     0|
    |  2162|NextSong|1513781065284|        0|     0|
    |  2162|NextSong|1513780860284|        0|     0|
    |  2162|NextSong|1513780569284|        0|     0|
    +------+--------+-------------+---------+------+
    only showing top 20 rows
    



```python
cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}).show()
```

    +------------------+
    |avg(count(period))|
    +------------------+
    | 6.898347107438017|
    +------------------+
    



```python

```
