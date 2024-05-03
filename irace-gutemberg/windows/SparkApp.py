from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit, col, concat_ws, regexp_replace, lower, sum
from nltk.corpus import stopwords
import pyspark.sql.functions as f
from datetime import datetime
from pathlib import Path
import nltk
import os
import re
from pyspark.sql import SparkSession

def build_session(parameters: dict):

    spark = SparkSession.builder\
                .master("local")\
                .appName('Irace')\
                .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0")\
                .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
                .config("spark.executor.memory", parameters.get('executorMemory', "1G"))\
                .config("spark.executor.instances", parameters.get('executorInstances', 2))\
                .config("spark.executor.cores", parameters.get('driverCores', 1))\
                .config("spark.sql.shuffle.partitions", parameters.get('sqlShufflePartitions', 200))\
                .config("spark.default.parallelism", parameters.get('defaultParallelism', 8))\
                .config("spark.storage.memoryFraction", parameters.get('memoryFraction', 0.6))\
                .config("spark.shuffle.compress", parameters.get('shuffleCompress', True))\
                .config("spark.sql.sources.partitionOverwriteMode", parameters.get('partitionOverwriteMode', 'static'))\
                .config("spark.cassandra.output.consistency.level", parameters.get('cassandraOutputConsistencyLevel', 'LOCAL_ONE'))\
                .config("spark.cassandra.input.split.sizeInMB", parameters.get('cassandraInputSplitSizeinMB', 64))\
                .config("spark.cassandra.output.batch.size.rows", parameters.get('cassandraOutputBatchSizeRows', 'auto'))\
                .config("spark.cassandra.output.batch.grouping.buffer.size", parameters.get('cassandraOutputBatchGroupingBufferSize', 1000))\
                .config("spark.cassandra.output.concurrent.writes", parameters.get('cassandraOutputConcurrentWrites', 5))\
                .getOrCreate()
    
    return spark


class SparkApp:
    def __init__(self, args:dict = None) -> None:
        self.parameters = args if args is not None else dict()
        self.spark, self.sc = self.spark_connection()
        self.cassandra_key_space = 'main_keyspace'
        self.cassandra_wordcount_source_table = 'word_count_source'
        self.cassandra_wordcount_table = 'word_count'

    def spark_connection(self):
        try:
            spark = build_session(self.parameters)
            
            sc=spark.sparkContext
            sc.setLogLevel("ERROR")

            return spark, sc  
        
        except Exception:
            print(f'Configuração não permitida, parâmetros: {self.parameters}')
    
    def spark_stop(self):
        self.spark.stop()
    
    def spark_config(self):
        spark_config = self.spark.sparkContext.getConf()

        return spark_config.getAll()
    
    @staticmethod
    def get_stop_words():
        return set(stopwords.words('english'))
    
    @staticmethod
    def build_wordcount_schema():
        schema = StructType([ \
                            StructField("word",StringType(),True), \
                            StructField("count",IntegerType(),True)
                        ])        
        return schema
    
    
    def list_files(self, path):        
        files = []
        dir = os.listdir(path) 

        for file in dir:
            files.append(file)

        return files
        
    def save_data_cassandra(self, df, keyspace, table, mode):
        df.write.format("org.apache.spark.sql.cassandra")\
              .option("confirm.truncate","true") \
              .options(table=table, keyspace=keyspace) \
              .mode(mode) \
              .save()
        
    def load_data_from_cassandra(self, key_space, table):
        df = self.spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace=key_space)\
            .load()
        
        return df
    
    @staticmethod
    def remove_special_characters(text):
        return re.sub(r'[^a-zA-Z0-9\s]', '', text)
    
    def transform_data(self, df):

        df = df.withColumn("source", regexp_replace("source", " ", "_"))\
        
        df = df.withColumn("source", lower(col("source")))

        df = df.dropna()
        df = df.filter(col('word')!='')

        df = df.filter(~df["word"].isin(self.get_stop_words()))

        return df        
    
    def word_count(self, path):
        files = self.list_files(path)
        schema = self.build_wordcount_schema()
        df = None

        for file in files:
            text_file = self.sc.textFile(f"{path}/{file}")

            text_file_cleaned = text_file.map(self.remove_special_characters)

            counts = text_file_cleaned.flatMap(lambda line: line.split(" ")) \
                                        .map(lambda word: (word.lower(), 1)) \
                                        .reduceByKey(lambda x, y: x + y)
        
            df_count = self.spark.createDataFrame(counts, schema=schema)
            df_count = df_count.withColumn('source', lit(file))

            df_count = self.transform_data(df_count)

            if df is None:
                df = df_count
            else:
                df = df.union(df_count)        
        
        df.count()

        df = df.withColumn("index_wordcount", concat_ws("_", df["source"], df["word"]))

        #self.save_data_cassandra(df, self.cassandra_key_space, self.cassandra_wordcount_source_table, 'overwrite')

        df = df.drop('source')
        df = df.drop('index_wordcount')

        df = df.groupBy("word").agg(f.sum("count").alias("count"))

        df.count()

        #self.save_data_cassandra(df, self.cassandra_key_space, self.cassandra_wordcount_table, 'overwrite')

        return datetime.now()
