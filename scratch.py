import pandas as pd
from sqlalchemy import create_engine
from PyInstaller.utils.hooks import collect_all

binaries, datas, hiddenimports = [], [], []

a, b, c = collect_all('xgboost')
a, b, c = collect_all('shap')
a, b, c = collect_all('pandas')
a, b, c = collect_all('matplotlib')
a, b, c = collect_all('gooey')

# from tqdm import tqdm
# import pyspark

# spark = pyspark.sql.SparkSession.Builder().getOrCreate()

conn_str1 = 'mssql://@spr'
engine1 = create_engine(conn_str1, echo=True)

conn_str2 = 'mssql://mj:Number1cat!@aviana.database.windows.net:1433/Revenew?Driver={SQL Server};'
engine2 = create_engine(conn_str2, echo=True)

df1 = pd.read_sql_table('Duplicate Reports', engine1)
df2 = pd.read_sql_table('invoice', engine1)

df1.sample(500).to_sql('Duplicate Reports', engine2, if_exists='replace', index=False)
df2.sample(500).to_sql('invoice', engine2, if_exists='replace', index=False, )

# file_name = 'C:\\Users\\MichaelJohnson\\AvianaML_dbo_Duplicate_Reports.csv\\AvianaML_dbo_Duplicate_Reports.csv'
# duplicate_reports = spark.read.csv(file_name, header=True, inferSchema=True,columnNameOfCorruptRecord='CorruptRecord', )
# df = duplicate_reports.toPandas()
# df.to_sql('Duplicate Reports', con, if_exists='replace', index=False,)

# file_name = 'C:\\Users\\MichaelJohnson\\AvianaML_dbo_invoice.csv\\AvianaML_dbo_invoice.csv'
# invoice = spark.read.csv(file_name, header=True, inferSchema=True,)
# df = invoice.sample(fraction=.0001).toPandas()
# df.to_sql('invoice', con, if_exists='replace', index=False)

# Log_Min_Abs_Amount_Mean
# Min_Abs_Amount_SDev
# Max_Abs_Amount_Mean
# Min_Abs_Amount_Mean
# Max_Abs_Amount_SDev
# Log_Max_Abs_Amount_Mean
