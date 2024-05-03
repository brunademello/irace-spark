from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import ipaddress

# Crie uma SparkSession
spark = SparkSession.builder \
    .appName("Extract IP Information") \
    .getOrCreate()

# Exemplo de DataFrame com endereços IP
data = [("6.141.237.155",),
        ("10.0.0.1",),
        ("172.16.0.1",)]
df = spark.createDataFrame(data, ["ip_address"])

# Função para extrair informações do IP
def extract_info(ip):
    ip_obj = ipaddress.ip_address(ip)
    network = ipaddress.ip_network(ip)
    version = ip_obj.version
    is_private = ip_obj.is_private
    broadcast = network.broadcast_address
    net_address = network.network_address
    subnet_mask = network.netmask
    ip_class = ipaddress.IPv4Network(ip).network_address
    return f"IP: {ip}, Network: {network}, Version: {version}, Is Private: {is_private}, Broadcast: {broadcast}, Network Address: {net_address}, Subnet Mask: {subnet_mask}, Class: {ip_class}"

# Registre a função como uma UDF
extract_info_udf = udf(extract_info, StringType())

# Aplique a função ao DataFrame
df = df.withColumn("ip_info", extract_info_udf(df["ip_address"]))

# Mostrar o DataFrame resultante
df.show(truncate=False)

# Feche a SparkSession
spark.stop()