from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import TimestampType

def test_spark():
    # Spark 애플리케이션 구성
    conf = SparkConf().setAppName("SparkTest")
    print('여기까지?')
    # 클러스터 관련 설정
    # # 예를 들어, Spark 클러스터의 마스터 URL이 'spark://master-url:7077'인 경우
    conf.setMaster("spark://spark-master:7077")

    # # 실행자 메모리와 코어 수 설정
    # conf.set("spark.executor.cores", "1")

    # # 로그 설정
    conf.set("spark.logConf", "true")

    # 추가적인 클러스터 특정 설정이 필요할 수 있음
    # 예: conf.set("spark.some.config.option", "value")

    # SparkContext 생성
    sc = SparkContext(conf=conf)
    print('여기까지2?')
    # 데이터 처리
    # data = [1, 2, 3, 4, 5]
    # rdd = sc.parallelize(data)
    # result = rdd.map(lambda x: x * 2).collect()


    rdd_x = sc.parallelize([("a", 1), ("b", 4), ("d", 5)])
    rdd_y = sc.parallelize([("a", 2), ("b", 3), ("c", 1)])
    print('여기까지3?')
    sorted(rdd_x.join(rdd_y).collect())
    print('여기까지4?')

    # print("처리 결과:", result)

    # SparkContext 종료
    sc.stop()

def naver_spark():

    endpoint_url = "https://kr.object.ncloudstorage.com"

    spark = SparkSession.builder.appName("csvToParquet") \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/airflow/aws-java-sdk-bundle-1.12.481.jar") \
    .config("spark.executor.extraClassPath", "/opt/airflow/aws-java-sdk-bundle-1.12.481.jar") \
    .getOrCreate()

    access_key = spark.sparkContext.getConf().get("spark.hadoop.fs.s3a.access.key")
    print('acess :: ', access_key)

    csv_file_path = "s3a://data-lake/bronze/eventsim.csv"
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    

    # 'ts' 필드를 날짜 형식으로 변환
    df = df.withColumn("date", to_date((col("ts") / 1000).cast(TimestampType())))

    # 날짜별로 데이터를 분할하고 Parquet으로 저장
    output_base_path = "s3a://data-lake/silver/eventsim/"
    df.write.partitionBy("date").parquet(output_base_path)

    spark.stop()





if __name__ == "__main__":
    naver_spark()
