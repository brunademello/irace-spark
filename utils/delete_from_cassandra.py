from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect()

session.execute("TRUNCATE TABLE analytics_data.logs")