from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, length, explode, split, max, concat_ws,  regexp_replace


spark = SparkSession.builder \
    .appName("Promedio de Puntuación y Longitud de Opiniones de Videojuegos por Género") \
    .getOrCreate()


df = spark.read.csv("/home/kinivera/BigData/Parcial2/input/games.csv", header=True)


df = df.withColumn("Rating", col("Rating").cast("float"))


df = df.withColumn("Genres", explode(split("Genres", ","))) \
       .withColumn("Genres", regexp_replace(col("Genres"), "\[|\]", ""))


# Filtrar para que todos los géneros tengan algún comentario o estén calificados
df_filtered = df.filter(col("Rating").isNotNull() & col("Reviews").isNotNull())

# Calcular el promedio de calificación
avg_rating_by_genre = df_filtered.groupBy("Genres").agg(avg("Rating").alias("PromedioPuntuacion"))

# Calcular el promedio del número de caracteres de las opiniones
avg_review_length_by_genre = df_filtered.groupBy("Genres").agg(avg(length(col("Reviews"))).cast("int").alias("PromedioLongitudOpiniones"))


avg_results = avg_rating_by_genre.join(avg_review_length_by_genre, "Genres")


# Convertir la columna PromedioPuntuacion a String
avg_results = avg_results.withColumn("PromedioPuntuacion", col("PromedioPuntuacion").cast("string"))

# Concatenar las dos columnas en una columna de texto
avg_results = avg_results.withColumn("text_data", concat_ws("\t", "Genres", "PromedioPuntuacion"))

# Guardar el DataFrame como un archivo de texto
avg_results.select("text_data").write.mode("overwrite").text("5_out.txt")

spark.stop()

