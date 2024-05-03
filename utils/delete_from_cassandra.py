from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect()

session.execute("TRUNCATE TABLE main_keyspace.logs_wordcount")