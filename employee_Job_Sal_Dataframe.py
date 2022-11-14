# importing necessary libraries

import pyspark

import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


from pyspark.sql  import SparkSession

# function to create new SparkSession
#def create_session():
spark = SparkSession.builder.master("local[1]") \
	.appName("Geek_examples.com") \
	.getOrCreate()
#return spark

# main function
#if __name__ == "__main__":

# calling function to create SparkSession
#spark = create_session()

# creating data for creating dataframe

jobData= [
("10001","Senior Engineer","1986-06-26","9999-01-01"),
("10002","Staff","1996-08-03","9999-01-01"),
("10003","Senior Engineer","1995-12-03","9999-01-01"),
("10004","Senior Engineer","1995-12-01","9999-01-01"),
("10005","Senior Staff","1996-09-12","9999-01-01")
]

salaryData= [
("10001","66074","1988-06-25","1989-06-25"), ("10001","62102","1987-06-26","1988-06-25"),
("10001","60117","1986-06-26","1987-06-26") , ("10002","72527","2001-08-02","9999-01-01"),
("10002","71963","2000-08-02","2001-08-02"), ("10002","69366","1999-08-03","2000-08-02") ,
("10003","43311","2001-12-01","9999-01-01"), ("10003","43699","2000-12-01","2001-12-01"),
("10003","43478","1999-12-02","2000-12-01") ,
("10004","74057","2001-11-27","9999-01-01"), ("10004","70698","2000-11-27","2001-11-27"),
("10004","69722","1999-11-28","2000-11-27"),
("10005","94692","2001-09-09","9999-01-01"), ("10005","91453","2000-09-09","2001-09-09"),
("10005","90531","1999-09-10","2000-09-09")
]


# giving schema



jobColumn = ["EMP_NO", "TITLE", "FROM_DATE" , "TO_DATE"] 

salaryColumn = ["EMP_NO", "SALARY", "FROM_DATE" , "TO_DATE"] 


# creating dataframe using createDataFrame()
# function in which pass data and schema
df = spark.createDataFrame(jobData,schema=jobColumn)
df1=spark.createDataFrame(salaryData,schema=salaryColumn)

# visualizing the dataframe using show() function
df.show()
df1.show()


dftot =df.join(df1,df.EMP_NO ==  df1.EMP_NO,"inner") \
     
dftot.show(truncate=False)

from pyspark.sql import functions as A
from pyspark.sql.functions import avg


dftot_num=dftot.select("TITLE", A.col("SALARY").cast("int"))


#dftot_num.groupBy("TITLE") \
#.agg(avg("SALARY").alias("avg_salary"))
#dftot_num.show(truncate=False)

dftot_num.groupBy("TITLE").agg({"SALARY":"avg"}).show()




df1_max_each=df1.select("EMP_NO", A.col("SALARY").cast("int"))

df1_max_each.groupBy("EMP_NO").agg({"SALARY":"max"}).show()


df1_really_max=df1_max_each.groupBy("EMP_NO").agg({"SALARY":"max"})

df1_really_max.show()


