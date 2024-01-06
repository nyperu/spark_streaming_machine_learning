from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# SparkSession oluştur
spark = SparkSession.builder \
    .appName("Salary Data Stream") \
    .getOrCreate()

# Kafka'dan veri okuma
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:90a92") \
    .option("subscribe", "salary_topic") \
    .load()
	
# Kafka verilerinin şemasını belirle
schema = StructType() \
    .add("Age", FloatType()) \
    .add("Gender", StringType()) \
    .add("Education Level", StringType()) \
    .add("Job Title", StringType()) \
    .add("Years of Experience", FloatType()) \
    .add("Salary", FloatType())

# JSON formatındaki veriyi şemaya uygun olarak dönüştür
df = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# StringIndexer ve VectorAssembler tanımlamaları
gender_indexer = StringIndexer(inputCol="Gender", outputCol="GenderIndex", handleInvalid="keep")
education_indexer = StringIndexer(inputCol="Education Level", outputCol="EducationLevelIndex", handleInvalid="keep")
job_title_indexer = StringIndexer(inputCol="Job Title", outputCol="JobTitleIndex", handleInvalid="keep")
assembler = VectorAssembler(inputCols=["Age", "GenderIndex", "EducationLevelIndex", "JobTitleIndex", "Years of Experience"], outputCol="features")

# Lineer Regresyon modeli ekle
lr = LinearRegression(featuresCol="features", labelCol="Salary")

# Pipeline oluştur
pipeline = Pipeline(stages=[gender_indexer, education_indexer, job_title_indexer, assembler, lr])

# Statik veri seti üzerinde modeli eğit
static_df = spark.read.csv("egitim.csv", header=True, inferSchema=True).na.drop()
model = pipeline.fit(static_df)

# Modeli kaydet
model_path = "saved_model"
model.write().overwrite().save(model_path)

# Akış verisine modeli uygula
# Modeli tekrar yükle
loaded_model = PipelineModel.load(model_path)
df_transformed = loaded_model.transform(df)
# Yüzdesel farkı hesaplayan fonksiyon
def calculate_accuracy(salary, prediction):
    if salary != 0:
        return abs((prediction - salary) / salary) * 100
    else:
        return None

# UDF olarak kaydet
calculate_accuracy_udf = udf(calculate_accuracy, DoubleType())

# UDF'yi DataFrame'e uygula ve yeni bir sütun olarak ekle
df_with_accuracy = df_transformed.withColumn("Accuracy", calculate_accuracy_udf("Salary", "prediction"))

# Ekrana yazdırmak için query başlat
query = df_with_accuracy.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
