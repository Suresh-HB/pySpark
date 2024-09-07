"""
@Author: Suresh <br>
@Date: 2024-09-05 <br>
@Last Modified by: Suresh <br>
@Last Modified: 2024-09-05 <br>
@Title : Read write operations using different file formats (csv , json)

"""


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Simple Test") \
    .getOrCreate()

input_path = "file:///C:/Users/Suresh/Desktop/pythonBl/pyspark/myData.csv"
df = spark.read.csv(input_path)


output_path = "file:///C:/Users/Suresh/Desktop/pythonBl/pyspark/simple_outpu"
df.coalesce(1).write.json(output_path)

spark.stop()
