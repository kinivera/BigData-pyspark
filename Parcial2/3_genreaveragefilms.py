import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F

sc = pyspark.SparkContext()

spark = SparkSession.builder \
    .appName("p3") \
    .getOrCreate()

#Cargar archivos 

movies = spark.read.csv("/home/kinivera/BigData/Parcial2/input/ml-25m/movies.csv", header=True)#movieId,title,genres
ratings = spark.read.csv("/home/kinivera/BigData/Parcial2/input/ml-25m/ratings.csv", header=True)#userId,movieId,rating,timestamp

#limpiar columnas 

movies= movies.withColumn("genre", F.explode(F.split("genres", "\\|")))#nueva entrada(fila) para cada genero mismos datos 

#movieId|               title|              genres|   genre|userId|rating|
#+-------+--------------------+--------------------+--------+------+------+
#|    296| Pulp Fiction (1994)|Comedy|Crime|Dram...|Thriller|     1|   5.0|
#|    296| Pulp Fiction (1994)|Comedy|Crime|Dram...|   Drama|     1|   5.0|
#|    296| Pulp Fiction (1994)|Comedy|Crime|Dram...|   Crime|     1|   5.0|
#|    296| Pulp Fiction (1994)|Comedy|Crime|Dram...|  Comedy|     1|   5.0|


movies = movies.drop("genres")
ratings = ratings.drop("timestamp")#userId,movieId,rating


#print(movies.show())
#print(ratings.show())


# Realizar el join

joined_df = movies.join(ratings, "movieId", "inner")
#joined_df.show()

#Calcular la calificación promedio y cuenta las películas por género
#1.agrupar por genre
#2.calcualr promedio usando rating
#3.Cuenta numero de valores unicos (movieId)

genre_avg_rating_count = joined_df.groupBy("genre") \
    .agg(F.avg("rating").alias("average_rating"), F.countDistinct("movieId").alias("movie_count")) \
    .orderBy("genre")
    
# Muestra los resultados
#genre_avg_rating_count.show()

resultados_txt = genre_avg_rating_count.collect()
# Escribir la cadena en un archivo de texto
with open("/home/kinivera/BigData/Parcial2/3_out.txt", "w") as f:
    for row in resultados_txt:
        f.write(f"{row['genre']}, {row['average_rating']}, {row['movie_count']}\n")

