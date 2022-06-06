import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import udf,variance
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession,Row,Column
import json
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
import sys
from pyspark.sql.functions import * 
from pyspark.ml.classification import NaiveBayes 
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LinearSVC

# Load training data
training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

lsvc = LinearSVC(maxIter=10, regParam=0.1)

# Fit the model
lsvcModel = lsvc.fit(training)

# Print the coefficients and intercept for linear SVC
print("Coefficients: " + str(lsvcModel.coefficients))
print("Intercept: " + str(lsvcModel.intercept))

# Read data from the test.csv 
vehicle_data = pd.read_csv(“./test.csv”, header=None)

#Sets the Spark master URL to run locally. 
spark = SparkSession.builder.master("local[*]").getOrCreate() 

#Create DataFrame 
vehicle_df = spark.createDataFrame(vehicle_data) vehicle_df.show(5)

vehicle_df = vehicle_df.select(col("0").alias("number_plate"),  col("1").alias("brand"),  
col("2").alias("color"),  
col("3").alias("time"),  
col("4").alias("stoled"))

indexers = [
StringIndexer(inputCol="brand", outputCol = "brand_index"),  
StringIndexer(inputCol="color", outputCol = "color_index"),  StringIndexer(inputCol="time", outputCol = "time_index"),  StringIndexer(inputCol="stoled", outputCol = "label")]
pipeline = Pipeline(stages=indexers) 
#Fitting a model to the input dataset. 
indexed_vehicle_df = ipeline.fit(vehicle_df).transform(vehicle_df) 
indexed_vehicle_df.show(5,False) 
#We have given False for turn off default truncation

vectorAssembler = VectorAssembler(inputCols = [“brand_index”, “color_index”, “tim e_index”],outputCol = “features”) vindexed_vehicle_df = vectorAssembler.transform(indexed_vehicle_df) vindexed_vehicle_df.show(5, False)

splits = vindexed_vehicle_df.randomSplit([0.6,0.4], 42) 
# optional value 42 is seed for sampling 
train_df = splits[0] 
test_df = splits[1]

nb = NaiveBayes(modelType=”multinomial”)

nbmodel = nb.fit(train_df)

predictions_df = nbmodel.transform(test_df)
predictions_df.show(5, True)

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="pr ediction", metricName="accuracy") 
nbaccuracy = evaluator.evaluate(predictions_df) 
print("Test accuracy = " + str(nbaccuracy))

sc = SparkContext("local[2]", "NetworkWordCount")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)
sql_context=SQLContext(sc)

lines = ssc.socketTextStream("localhost", 6100)

def get_prediction(tweet_text):
	try:
                # remove the blank tweets
		tweet_text = tweet_text.filter(lambda x: len(x) > 0)
                # create the dataframe with each row contains a tweet text
		rowRdd = tweet_text.map(lambda w: Row(tweet=w))
		wordsDataFrame = spark.createDataFrame(rowRdd)
		# get the sentiments for each row
		print(wordsDataFrame)
		#pipelineFit.transform(wordsDataFrame).select('tweet','prediction').show()
	except : 
		print('No data')

    # define the schema
my_schema = tp.StructType([
				tp.StructField(name= 'id',          dataType= tp.IntegerType(),  nullable= True),   				
    				tp.StructField(name= 'tweet',       dataType= tp.StringType(),   nullable= True)    
    			      ])		
		
#print('\n\nReading the dataset...........................\n')
#my_data = spark.read.csv('/home/pes2ug19cs013/Desktop/project/sentiment/test.csv', schema=my_schema, header=True)
#my_data.show(2)

#my_data.printSchema()

#print(lines)

words = lines.flatMap(lambda line : line.split(" "))
words.foreachRDD(get_prediction)

ssc.start()             # Start the computation
ssc.awaitTermination()


sc = SparkContext.getOrCreate()
sc.setLogLevel("OFF")
ssc = StreamingContext(sc, 1)
spark=SparkSession(sc)

# Create a DStream that will connect to hostname:port, like localhost:9999
data = ssc.socketTextStream("localhost", 6100)
#data.pprint()
#print("hi")

try:
	def readMyStream(rdd):
		df=spark.read.json(rdd)
		print(df)
except Exception as e:
	print(e)
#print('hello')

try:
	data.foreachRDD(lambda rdd: readyMyStream(rdd))
except Exception as e:
	print(e)

ssc.start()
time.sleep(1000)
ssc.stop(stopSparkContext=False)