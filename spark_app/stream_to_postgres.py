# Import packages 
import os
from pyspark.sql import SparkSession, functions as F, types as T

# Get environment variables and create postgres connection
pg_url = f"jdbc:postgresql://{os.getenv('PGHOST','postgres')}:{os.getenv('PGPORT','5432')}/{os.getenv('PGDATABASE', 'flight_pipeline')}"

# Database user credential
pg_user = os.getenv("PGUSER", "flight_USER")
pg_pass = os.getenv("PGPASSWORD", "P@ssword123")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/chk/flights_stream")

# Initialize spark session with postgreSQL JDBC driver
spark = (SparkSession.builder
        .appName("flights_stream")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
)

# Define streaming dataframe reading from kafka topic
raw = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP","flight_kafka:9092"))
      .option("subscribe", os.getenv("KAFKA_TOPIC", "flights_live"))
      .option("startingOffsets","latest")
      .load()
)

# Schema that matches your producer's JSON
schema = T.StructType([
    T.StructField("flight_key",   T.StringType()),
    T.StructField("flight_date",  T.StringType()),
    T.StructField("status",       T.StringType()),
    T.StructField("airline", T.StructType([
        T.StructField("iata", T.StringType()),
        T.StructField("icao", T.StringType()),
        T.StructField("name", T.StringType())
    ])),
    T.StructField("flight", T.StructType([
        T.StructField("number", T.StringType()),
        T.StructField("iata",   T.StringType()),
        T.StructField("icao",   T.StringType())
    ])),
    T.StructField("departure", T.StructType([
        T.StructField("airport",   T.StringType()),
        T.StructField("iata",   T.StringType()),
        T.StructField("icao",   T.StringType()),
        T.StructField("gate",      T.StringType()),
        T.StructField("terminal",  T.StringType()),
        T.StructField("schedule",  T.StringType()),
        T.StructField("estimated", T.StringType()),
        T.StructField("actual",    T.StringType()),
        T.StructField("delay_min", T.IntegerType())
    ])),
    T.StructField("arrival", T.StructType([
        T.StructField("airport",   T.StringType()),
        T.StructField("iata",   T.StringType()),
        T.StructField("icao",   T.StringType()),
        T.StructField("gate",      T.StringType()),
        T.StructField("terminal",  T.StringType()),
        T.StructField("schedule",  T.StringType()),
        T.StructField("estimated", T.StringType()),
        T.StructField("actual",    T.StringType()),
        T.StructField("delay_min", T.IntegerType())
    ])),
    T.StructField("ingest_time", T.StringType()),
    T.StructField("source",      T.StringType())
])

TS_FMT = "yyyy-MM-dd'T'HH:mm:ssXXX"

# Parse Kafka value → JSON → structured columns
parsed = (
    raw
      .select(F.col("value").cast("string").alias("json"))          # Kafka value is bytes → string
      .select(F.from_json("json", schema).alias("r"))               # parse JSON using schema
      .select("r.*")                                                # expand struct to top-level
      .withColumn("dep_sched_ts", F.to_timestamp(F.col("departure.schedule"), TS_FMT))
      .withColumn("dep_est_ts", F.to_timestamp(F.col("departure.estimated"), TS_FMT))
      .withColumn("dep_act_ts", F.to_timestamp(F.col("departure.actual"), TS_FMT))
      .withColumn("arr_sched_ts", F.to_timestamp(F.col("arrival.schedule"), TS_FMT))
      .withColumn("arr_est_ts", F.to_timestamp(F.col("arrival.estimated"), TS_FMT))
      .withColumn("arr_act_ts", F.to_timestamp(F.col("arrival.actual"), TS_FMT))
      .withColumn("ingest_ts", F.to_timestamp(F.col("ingest_time"), TS_FMT))
      .withColumn("dep_delay_min", F.col("departure.delay_min").cast("double"))
      .withColumn("arr_delay_min", F.col("arrival.delay_min").cast("double"))
      .withColumn(
          "arr_eta_delta_min",
          (F.col("arr_est_ts").cast("long") - F.col("arr_sched_ts").cast("long")) / 60.0
      )
      .withColumn("airline_iata", F.col("airline.iata"))
      .withColumn("airline_icao", F.col("airline.icao"))
      .withColumn("airline_name", F.col("airline.name"))
      .withColumn("flight_number", F.col("flight.number"))
      .withColumn("flight_iata",   F.col("flight.iata"))
      .withColumn("flight_icao",   F.col("flight.icao"))
      .withColumn("dep_airport",   F.col("departure.airport"))
      .withColumn("dep_airport_iata",   F.col("departure.iata"))
      .withColumn("dep_airport_icao",   F.col("departure.icao"))
      .withColumn("dep_gate",      F.col("departure.gate"))
      .withColumn("dep_terminal",  F.col("departure.terminal"))
      .withColumn("arr_airport",   F.col("arrival.airport"))
      .withColumn("arr_airport_iata",   F.col("arrival.iata"))
      .withColumn("arr_airport_icao",   F.col("arrival.icao"))
      .withColumn("arr_gate",      F.col("arrival.gate"))
      .withColumn("arr_terminal",  F.col("arrival.terminal"))
)

# Select columns and and write data into the staging table of the postgres database
def write_batch(df, _):
    (
        df.select(
            F.col("flight_key"),
            F.to_date("flight_date").alias("flight_date"),
            F.col("status"),
            F.coalesce(F.col("ingest_ts"), F.to_timestamp("ingest_time")).alias("ingest_time"),
            F.col("flight_number").alias("flight_number"),
            F.col("flight_iata").alias("flight_iata"),
            F.col("flight_icao").alias("flight_icao"),
            F.col("airline_iata").alias("airline_iata"),
            F.col("airline_name").alias("airline_name"),
            F.col("dep_airport").alias("dep_airport"),
            F.col("dep_airport_iata").alias("dep_airport_iata"),
            F.col("dep_airport_icao").alias("dep_airport_icao"),
            F.col("dep_terminal").alias("dep_terminal"),
            F.col("dep_gate").alias("dep_gate"),
            F.col("dep_sched_ts").cast("timestamp").alias("dep_scheduled"),
            F.col("dep_est_ts").cast("timestamp").alias("dep_estimated"),
            F.col("dep_act_ts").cast("timestamp").alias("dep_actual"),
            F.col("dep_delay_min").cast("double").alias("dep_delay_min"),
            F.col("arr_airport").alias("arr_airport"),
            F.col("arr_airport_iata").alias("arr_airport_iata"),
            F.col("arr_airport_icao").alias("arr_airport_icao"),
            F.col("arr_terminal").alias("arr_terminal"),
            F.col("arr_gate").alias("arr_gate"),
            F.col("arr_sched_ts").cast("timestamp").alias("arr_scheduled"),
            F.col("arr_est_ts").cast("timestamp").alias("arr_estimated"),
            F.col("arr_act_ts").cast("timestamp").alias("arr_actual"),
            F.col("arr_delay_min").cast("double").alias("arr_delay_min"),
        )
        .write
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "fact_flight_status_staging")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "5000")
        .option("isolationLevel", "READ_COMMITTED")
        .mode("append")
        .save()
    )

# Start streaming    
query = (
    parsed.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .foreachBatch(write_batch)
    .start()
)
print(">>> Streaming query started; checkpoint =", CHECKPOINT, flush=True)
query.awaitTermination()

