from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, length
from pyspark.sql.functions import concat_ws


spark = SparkSession.builder \
    .appName("Promedio de Puntuación y Longitud de Opiniones de Videojuegos por Género") \
    .getOrCreate()


df = spark.read.csv("/home/kinivera/BigData/Parcial2/input/games.csv", header=True)

# Convertir la columna "Rating" a tipo de dato float
df = df.withColumn("Rating", col("Rating").cast("float"))

# Filtrar para que todos los generos tengan algun comentario o este calificado
df_filtered = df.filter(col("Rating").isNotNull() & col("Reviews").isNotNull())

# Calcular el promedio de calificaion
avg_rating_by_genre = df_filtered.groupBy("Genres").agg(avg("Rating").alias("PromedioPuntuacion"))

# Calcular el promedio del número de caracteres de las opiniones
avg_review_length_by_genre = df_filtered.groupBy("Genres").agg(avg(length(col("Reviews"))).alias("PromedioLongitudOpiniones"))

avg_results = avg_rating_by_genre.join(avg_review_length_by_genre, "Genres")


# Convertir la columna PromedioPuntuacion a String
avg_results = avg_results.withColumn("PromedioPuntuacion", col("PromedioPuntuacion").cast("string"))

# Concatenar las dos columnas en una columna de texto
avg_results = avg_results.withColumn("text_data", concat_ws("\t", "Genres", "PromedioPuntuacion"))

# Guardar el DataFrame como un archivo de texto
avg_results.select("text_data").write.mode("overwrite").text("5_out.txt")



spark.stop()

