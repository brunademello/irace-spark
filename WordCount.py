import findspark
findspark.init("/mnt/spark/")

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType
from pyspark.sql.functions import lit, col, udf
from pyspark.sql import SparkSession
import datetime
import uuid
import os


class WordCountSparkCassandraIrace:
    def __init__(self, args:dict = None) -> None:
        self.parameters = args if args is not None else dict()
        self.spark, self.sc = self.spark_connection()
        self.cassandra_key_space = 'analytics_data'
        self.cassandra_wordcount_table = 'wordcount'
        self.cassandra_metadata_table = 'irace_metadata'
        self.cassandra_logs_table = 'logs_wordcount'

    def spark_connection(self):
        try:
            spark = SparkSession.builder\
                        .master("local")\
                        .appName('Word Count - Spark&Cassandra&iRace')\
                        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0")\
                        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
                        .config("spark.executor.memory", self.parameters.get('memory', '1g'))\
                        .config("spark.executor.cores", self.parameters.get('cores', 1))\
                        .config("spark.sql.shuffle.partitions", self.parameters.get('shufflePartitions', 1))\
                        .config("spark.default.parallelism", self.parameters.get('parallelism', 8))\
                        .config("spark.speculation", self.parameters.get('speculation', 'false'))\
                        .config("spark.sql.sources.partitionOverwriteMode", self.parameters.get('partitionOverwriteMode', 'static'))\
                        .config("spark.cassandra.output.consistency.level", self.parameters.get('cassandra_output_consistency_level', 'LOCAL_ONE'))\
                        .config("spark.cassandra.input.split.sizeInMB", self.parameters.get('cassandra_input_split_size_in_mb', 64))\
                        .config("spark.cassandra.output.batch.size.rows", self.parameters.get('cassandra_input_batch_size_rows', "auto"))\
                        .config("spark.cassandra.output.batch.grouping.buffer.size", self.parameters.get('cassandra_input_batch_grouping_buffer_size', 1000))\
                        .getOrCreate()
            
            sc=spark.sparkContext
            sc.setLogLevel("ERROR")

            return spark, sc  
        except Exception:
            print(f'Configuração não permitida, parâmetros: {self.parameters}')
    
    def spark_stop(self):
        self.spark.stop()
    
    @staticmethod
    def build_wordcount_schema():
        schema = StructType([ \
                            StructField("word",StringType(),True), \
                            StructField("count",IntegerType(),True)
                        ])        
        return schema
    
    @staticmethod
    def build_metadata_schema():
        schema = StructType([ \
                            StructField("execution_id",IntegerType(),True), \
                            StructField("instance_id",IntegerType(),True), \
                            StructField("configuration_id",IntegerType(),True), \
                            StructField("parameters",StringType(),True), \
                            StructField("begin_timestamp",TimestampType(),True), \
                            StructField("end_timestamp",TimestampType(),True), \
                            StructField("total_time_seconds",FloatType(),True), \
                            StructField("execution_status",BooleanType(),True)
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
    
    def irace_save_metadata(self, execution_id, instance_id, configuration_id, parameters, begin, end, total, execution_status):
        
        schema = self.build_metadata_schema()
        data = [(execution_id, int(instance_id), int(configuration_id), parameters, begin, end, total, execution_status)]

        df = self.spark.createDataFrame(data, schema=schema)

        uuid_udf = udf(lambda : str(uuid.uuid4()), StringType())

        df = df.withColumn('id', uuid_udf())

        df = df.select('id', 'execution_id', 'instance_id', 
                       'configuration_id', 'parameters', 
                       'begin_timestamp', 'end_timestamp', 'total_time_seconds', 'execution_status')
    

        self.save_data_cassandra(df, self.cassandra_key_space, self.cassandra_metadata_table, 'append')
    
    def word_count(self, path):
        files = self.list_files(path)
        schema = self.build_wordcount_schema()
        df = None

        for file in files:
            text_file = self.sc.textFile(f"{path}/{file}")

            counts = text_file.flatMap(lambda line: line.split(" ")) \
                              .map(lambda word: (word.lower(), 1)) \
                              .reduceByKey(lambda x, y: x + y)
        
            df_count = self.spark.createDataFrame(counts, schema=schema)
            df_count = df_count.withColumn('source', lit(file))

            df_count = df_count.dropna()
            df_count = df_count.filter(col('word')!='')

            if df is None:
                df = df_count
            else:
                df = df.union(df_count)

        self.save_data_cassandra(df, self.cassandra_key_space, self.cassandra_metadata_table, 'overwrite')

    def word_count_logs(self, path):
        files = self.list_files(path)
        schema = self.build_wordcount_schema()

        for file in files:
            print(file)
            text_file = self.sc.textFile(f"{path}/{file}")

            counts = text_file.flatMap(lambda line: line.split(" ")) \
                              .map(lambda word: (word.lower(), 1)) \
                              .reduceByKey(lambda x, y: x + y)
        
            df_count = self.spark.createDataFrame(counts, schema=schema)

            df_count = df_count.dropna()
            df_count = df_count.filter(col('word')!='')

            self.save_data_cassandra(df_count, self.cassandra_key_space, self.cassandra_logs_table, 'overwrite')

                    
