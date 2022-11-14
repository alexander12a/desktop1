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

#dftot_num.groupBy("TITLE").agg({"SALARY":"avg"}).show()


dfavg=dftot_num.groupBy("TITLE").avg("SALARY").withColumnRenamed("avg(SALARY)", "avg_salary")
dfavg.show()


dfx=df1.select("EMP_NO", A.col("SALARY").cast("int"))


#maximum salary each emp

#dfx.groupBy("EMP_NO") \
 # .max("SALARY") \
  #.withColumnRenamed("MAX(SALARY)", "max_salary")\
  #.show()

dfy=dfx.groupBy("EMP_NO").max("SALARY").withColumnRenamed("MAX(SALARY)", "max_salary")
  
dfy.show()



#df_salary_maxjoin =dfx.join(df1,dfx.EMP_NO ==  df1.EMP_NO,"inner").drop(df1.EMP_NO) \

df_salary_maxjoin =dfy.join(df1,['EMP_NO'])

df_salary_maxjoin.select("EMP_NO", "SALARY", "FROM_DATE" , "TO_DATE","max_salary").show()

dfu=df.select("TITLE", "EMP_NO")



df_salary_maxjoin2 =dfu.join(df_salary_maxjoin,['EMP_NO'])
df_salary_maxjoin2.show()


df_salary_maxjoin3 =dfavg.join(df_salary_maxjoin2,['TITLE'])
df_salary_maxjoin3.show()

dft =dfavg.join(df_salary_maxjoin2,['TITLE'])

from pyspark.sql.functions import when, col
#conditions = when(col("max_salary") < (col("avg_salary"), "True"))
             #when(col("max_salary") > (col("avg_salary"), "False"))
#df_salary_maxjoin3 = df_salary_maxjoin3.withColumn("lower Average", when(col("max_salary") < (col("avg_salary"), "True")))

dft=dft.select( "TITLE","avg_salary","EMP_NO","max_salary","FROM_DATE","TO_DATE", A.col("SALARY").cast("int"))
dft = dft.withColumn("LOWER AVERAGE", when(dft.SALARY < dft.avg_salary, "True")
									 .when(dft.SALARY > dft.avg_salary, "False"))	
dft.show()


from pyspark.sql.functions import col 
#to save data
dft2=dft.select("EMP_NO","SALARY",col("FROM_DATE").alias("FROM DATE"),col("TO_DATE").alias("TO DATE"),"LOWER AVERAGE") 
dft2.show()

#dft2.rdd.saveAsPickleFile(salaryData)
#
#dft2.write.csv(r"C:\Users\gill2\AppData\Local\Programs\Python\Python310\salaryx.csv")

#dft2.write.format("csv").mode("overwrite").save(r"C:\file12\salaryx.csv")
import pyspark.pandas 
dft2.toPandas().to_csv(r'c:\file12\mycsv.csv')