import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import pyspark

spark = pyspark.sql.SparkSession.Builder().getOrCreate()
conn_str = 'mssql://@spr'
engine = create_engine(conn_str)
con = engine.connect()

file_name = 'C:\\Users\\MichaelJohnson\\AvianaML_dbo_Duplicate_Reports.csv\\AvianaML_dbo_Duplicate_Reports.csv'
duplicate_reports = spark.read.csv(file_name, header=True, inferSchema=True,columnNameOfCorruptRecord='CorruptRecord')
df = duplicate_reports.toPandas()
df.to_sql('Duplicate Reports', con, if_exists='replace', index=False,)

file_name = 'C:\\Users\\MichaelJohnson\\AvianaML_dbo_invoice.csv\\AvianaML_dbo_invoice.csv'
invoice = spark.read.csv(file_name, header=True, inferSchema=True,)
df = invoice.sample(fraction=.0001).toPandas()
df.to_sql('invoice', con, if_exists='replace', index=False)

# Log_Min_Abs_Amount_Mean
# Min_Abs_Amount_SDev
# Max_Abs_Amount_Mean
# Min_Abs_Amount_Mean
# Max_Abs_Amount_SDev
# Log_Max_Abs_Amount_Mean