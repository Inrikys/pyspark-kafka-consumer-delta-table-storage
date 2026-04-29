from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Cria sessão Spark com suporte a Kafka
spark = SparkSession.builder \
    .appName("KafkaConsumerExample") \
    .getOrCreate()

# Configuração do Kafka
kafka_bootstrap_servers = "localhost:9092"
topic = "test-topic"

# Lê do Kafka como stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka retorna key/value em binário → converter pra string
parsed_df = df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)

# Exibir no console (modo debug)
query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()