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
employeeData = [
("10001","1953-09-02","Georgi","Facello","M","1986-06-26"),
("10002","1964-06-02","Bezalel","Simmel","F","1985-11-21"),
("10003","1959-12-03","Parto","Bamford","M","1986-08-28"),
("10004","1954-05-01","Chirstian","Koblick","M","1986-12-01"),
("10005","1955-01-21","Kyoichi","Maliniak","M","1989-09-12")
]

# giving schema


employeeColumn = ["EMP NO", "BIRTH_DATE", "FIRST_NAME", "LAST_NAME", "GENDER", "HIRE DATE"]


#salaryColumn = ["EMP NO", "TITLE", "FROM_DATE" , "TO_DATE"] 
# schm=["Name of employee","Gender","Salary","Years of experience"]

# creating dataframe using createDataFrame()
# function in which pass data and schema
df = spark.createDataFrame(employeeData,schema=employeeColumn)

# visualizing the dataframe using show() function
df.show()

from pyspark.sql.functions import concat,col, lit
from pyspark.sql.functions import substring
from pyspark.sql.functions import to_date
from pyspark.sql.functions import date_format

df.select(concat(col("FIRST_NAME").substr(1, 2),col("LAST_NAME"),lit("@company.com")).alias("Details_email"),"EMP NO",date_format("BIRTH_DATE", "dd.MMM.yyyy").alias("BIRTH DATE"), "FIRST_NAME", "LAST_NAME", "GENDER", "HIRE DATE").show()



