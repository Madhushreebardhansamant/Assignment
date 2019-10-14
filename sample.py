import configparser
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('sample.properties')
config.sections()

spark = SparkSession.builder.appName("demo").master("local")\
    .config("spark.jars", "/home/fission/.ivy2/jars/mysql_mysql-connector-java-8.0.12.jar")\
    .getOrCreate()

dbURL="jdbc:mysql://"+ config['Dev']['ip_address'] \
      +":"+config['Dev']['port_number']+\
      "/retail_db?" \
      "useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

df = spark.read.format("jdbc") \
     .options(url=dbURL,
              database=config['Dev']['database'],
              dbtable=config['Dev']['dbtable'],
              user=config['Dev']['user_name'],
              password=config['Dev']['password'],
              driver='com.mysql.cj.jdbc.Driver') \
     .load();

df1 = df.groupBy(['order_item_quantity']).count()
df2 = df.groupBy(["order_item_product_id","order_item_product_price"]).count()

df3 = df2.coalesce(1).write.format('json').save('/home/fission/Desktop/assignemnt/file_name.json')

