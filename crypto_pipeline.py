import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp
from pyspark.sql.types import *

spark = (
    SparkSession.builder.appName("crypto-pipeline")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
    .getOrCreate()
)

url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1,
    "sparkline": "false",
}

response = requests.get(url, params=params)
data = response.json()

clean_data = []

for coin in data:
    clean_data.append(
        {
            "coin_id": coin.get("id"),
            "symbol": coin.get("symbol"),
            "name": coin.get("name"),
            "current_price": float(coin.get("current_price") or 0),
            "market_cap": float(coin.get("market_cap") or 0),
            "market_cap_rank": int(coin.get("market_cap_rank") or 0),
            "total_volume": float(coin.get("total_volume") or 0),
            "price_change_24h": float(coin.get("price_change_24h") or 0),
            "price_change_percentage_24h": float(
                coin.get("price_change_percentage_24h") or 0
            ),
            "last_updated": coin.get("last_updated"),
        }
    )

schema = StructType(
    [
        StructField("coin_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("market_cap_rank", IntegerType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("last_updated", StringType(), True),
    ]
)

df = spark.createDataFrame(clean_data, schema=schema)

df = df.withColumn("last_updated", to_timestamp(col("last_updated")))

df = df.withColumn("snapshot_time", current_timestamp())

db_url = "jdbc:postgresql://localhost:5433/crypto_dw"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

df.write.mode("append").jdbc(url=db_url, table="crypto_prices", properties=properties)

print("✅ Data inserted successfully")
