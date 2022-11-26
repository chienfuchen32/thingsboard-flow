from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
START_TS = 1668297600369
END_TS = 1668297605373
SPARK_MASTER = 'local[1]'

KEYSPACE = 'thingsboard'
TABLE = 'ts_kv_cf'
ENTITY_TYPE = 'DEVICE'
ENTITY_ID = 'cd02db80-4dfb-11ed-99b0-29091fb71b9b'

spark = SparkSession.builder.master(SPARK_MASTER) \
                    .appName("AggTsTB.com") \
                    .getOrCreate()

df = spark.read.format("org.apache.spark.sql.cassandra") \
               .options(keyspace=KEYSPACE, table=TABLE).load()
df_filter = df.where((df.entity_type == ENTITY_TYPE) &
                     (df.entity_id == ENTITY_ID) &
                     (df.ts >= START_TS) &
                     (df.ts <= END_TS))
df_filter.select(avg('dbl_v')).show()
