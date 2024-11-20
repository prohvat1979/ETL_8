"""Пример работы с тремя csv файлами с помощью PySpark, из которых необходимо извлечь информацию, объединить ее в одну таблицу,
привести даты к одному виду, удалить невалидные колонки, привести все валюты к одной и загрузить в БД MySQL. Все это добавить в DAG и запустить его.
"""

# импорт библиотек
import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp, date_format, to_date, regexp_replace, overlay
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType)
from datetime import datetime
warnings.filterwarnings("ignore")
con=create_engine("mysql://root:D8rfvqzlo@localhost/spark")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Hi").getOrCreate()

# загрузка файлов в датафреймы
booking_df = spark.read.format("csv")\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .load("D:\Geek_Brains\ETL\Семинар 08\ooking.csv")

client_df = spark.read.format("csv")\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .load("D:\Geek_Brains\ETL\Семинар 08\client.csv")

hotel = spark.read.format("csv")\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .load("D:\Geek_Brains\ETL\Семинар 08\hotel.csv")

hotel_df = hotel.withColumnRenamed('name', 'hotel_name')

# вывод датафреймов на печать
booking_df.show()
client_df.show()
hotel_df.show()

# объединение датафремов в одну таблицу
new_df = booking_df.join(client_df, 'client_id').join(hotel_df, 'hotel_id')
new_df.show()

# приведение даты к одному виду
new_df_1 = new_df.withColumn("booking_date", regexp_replace("booking_date", "/", "-"))
new_df_2 = new_df_1.withColumn("booking_date", to_date("booking_date"))
new_df_2.show()


# приведение валюты к одному виду

gbp_df = new_df_2.filter(col('currency').startswith('GBP'))
eur_df = new_df_2.filter(col('currency').startswith('EUR'))

# перевод фунтов в евро
gbp_changing = gbp_df.withColumn('booking_cost', new_df_2.booking_cost * 1.17)
gbp_in_eur = gbp_changing.withColumn("currency", regexp_replace("currency", "GBP", "EUR"))
new_df_3 = gbp_in_eur.union(eur_df)

new_df_3.show()

# запись датафрейма в MySQL
new_df_3.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=D8rfvqzlo")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "s8_table")\
        .mode("overwrite").save()

