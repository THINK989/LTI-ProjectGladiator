from json import loads


class KafkaLoans:
    def __init__(self, input_path=None):
        self.input_path = input_path

    def extract(self):
        return preprocess(spark.sql("select * from worldbankloan_db.loanstatement"))

    def load(self, df):
        if not os.path.exists("data_csv"):
            df.repartition("region").write.mode("overwrite").options(
                header="true", inferSchema="true", delimiter=","
            ).csv(f"file:///{str(Path(__file__).parent)}/data_csv")

    def kafka_stream_in(self):
        self.load(self.extract())
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: dumps(x).encode("utf-8"),
        )
        files = [file for file in os.listdir("data_csv") if file.endswith(".csv")]
        for file in files:
            with open(f"data_csv/{file}", mode="r") as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader, None)
                for row in csv_reader:
                    data = dumps(row)
                    producer.send("wb_loans", value=data)

        consumer = KafkaConsumer(
            "wb_loans",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: loads(x.decode("utf-8")),
        )
        for message in consumer:
            print(
                "%s:%d:%d: key=%s value=%s"
                % (
                    message.topic,
                    message.partition,
                    message.offset,
                    message.key,
                    message.value,
                )
            )

    # run ETL function
    def run(self):
        self.kafka_stream_in()


if __name__ == "__main__":
    import argparse, os, csv
    import findspark

    findspark.init()
    from json import dumps
    from pyspark.sql import SparkSession
    from kafka import KafkaProducer, KafkaConsumer
    import pyspark.sql.functions as F
    from pathlib import Path
    from data_clean import preprocess

    # Used to make it work with python directly without involving spaprk-submit
    spark = (
        SparkSession.builder.appName("Bank_Loan")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
        )
        .enableHiveSupport()
        .getOrCreate()
    )
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_path",
        type=str,
        default="data/IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv",
    )
    args = parser.parse_args()
    bk = KafkaLoans(input_path=args.input_path)
    bk.run()
