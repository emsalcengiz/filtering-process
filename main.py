from pyspark.sql.window import Window
from pyspark.sql.functions import col, asc,desc, row_number, split


split_date = split(df['Date'], ' ')

result_df = df.select(
    'ilan_id',
    'Price',
    split_date.getItem(0).alias("Date"),
    split_date.getItem(1).alias("Hour"),
    'Status',
    'KM',
    'Year'
)
column_list = ["ilan_id", "Status", "Date"]
status_list = ["Publish", "Unpublish"]

windowSpec  = Window.partitionBy([col(x) for x in column_list])\
        .orderBy(col("Hour").desc())

result_df.filter(result_df.Status.isin(status_list))\
        .withColumn("rn",row_number().over(windowSpec)).filter("rn == 1")\
        .drop("rn").orderBy("ilan_id","Date","Hour").show(141, truncate=False)
