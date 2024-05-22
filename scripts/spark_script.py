from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr

spark = SparkSession.builder \
    .appName("transformation").getOrCreate()

df = spark.read.format('csv') \
    .option('header','ture') \
        .option('inferSchema','true') \
            .load('s3://food-delivery12/food_delivery_data/')

df= df.withColumn('order_time',expr("to_timestamp(order_time,'yyyy-MM-dd HH:mm:ss')"))
df= df.withColumn('delivery_time',expr("to_timestamp(delivery_time,'yyyy-MM-dd HH:mm:ss')"))
df = df.filter(col('order_value')>0 & col('order_time').isNOtNull() & col('delivery_time').isNOtNull())


df = df.withColumn('delivery_duration', (col('order_time').cast('long')-col('delivery_time').cast('long'))/60)
df = df.withColumn('order_category',expr("case when order_value<20 then 'low' when order_value < 40 then 'Medium' else 'High' end "))

df.write.format('jdbc') \
    .option("url","jdbc:redshift://default-workgroup.516969323873.us-east-1.redshift-serverless.amazonaws.com:5439/food_db/food_delivery") \
    .option("dbtable","staging_food_delivery_data") \
    .option("user","admin") \
    .option("password","root1234A") \
    .option("driver","com.amazon.redshift.jdbc.Driver") \
    .mode('append') \
    .save()

spark.stop()

# jdbc:redshift://your-redshift-host:5439/your-database-name
