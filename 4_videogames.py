from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.functions import concat_ws


spark = SparkSession.builder \
    .appName("Videojuegos Populares") \
    .getOrCreate()


df = spark.read.csv("/home/kinivera/BigData/Parcial2/input/games.csv", header=True)


df = df.withColumn("Rating", col("Rating").cast("float"))
df = df.withColumn("Number of Reviews", col("Number of Reviews").cast("int"))
df = df.withColumn("Wishlist", col("Wishlist").cast("int"))
df = df.withColumn("Plays", col("Plays").cast("int"))

# Calcular el puntaje de popularidad sumando Rating, Number of Reviews, Wishlist y Plays
df = df.withColumn("PopularityScore", col("Rating") + col("Number of Reviews") + col("Wishlist") + col("Plays"))


# Determinar el juego más popular por género

# Agrupar el DataFrame por la columna "Genres" y calcular la puntuación de popularidad máxima para cada género
popular_by_genre = df.groupBy("Genres").agg(max("PopularityScore").alias("MaxPopularityScore"))

# Realizar una unión entre el DataFrame original y el DataFrame de juegos más populares por género
# La unión se realiza en las filas donde el género y la puntuación de popularidad coinciden
popular_games_genre = df.alias("df1").join(popular_by_genre.alias("df2"), 
                        (col("df1.Genres") == col("df2.Genres")) & 
                        (col("df1.PopularityScore") == col("df2.MaxPopularityScore"))) \
                        .select(col("df1.Genres"), col("df1.Title"), col("df1.PopularityScore"))

# Convertir la columna PopularityScore a String
popular_games_genre = popular_games_genre.withColumn("PopularityScore", col("PopularityScore").cast("string"))

# Concatenar las tres columnas en una columna de texto
popular_games_genre = popular_games_genre.withColumn("text_data", concat_ws("\t", *popular_games_genre.columns))

# Guardar el DataFrame como un archivo de texto
popular_games_genre.select("text_data").write.mode("overwrite").text("4_genreout.txt")


# Determinar el juego más popular por equipo de desarrollo
popular_by_team = df.groupBy("Team").agg(max("PopularityScore").alias("MaxPopularityScore"))
popular_games_team = df.alias("df1").join(popular_by_team.alias("df2"), (col("df1.Team") == col("df2.Team")) & (col("df1.PopularityScore") == col("df2.MaxPopularityScore"))) \
                        .select(col("df1.Team"), col("df1.Title"), col("df1.PopularityScore"))


# Convertir la columna PopularityScore a String
popular_games_team = popular_games_team.withColumn("PopularityScore", col("PopularityScore").cast("string"))

# Concatenar las tres columnas en una columna de texto
popular_games_team = popular_games_team.withColumn("text_data", concat_ws("\t", "Team", "Title", "PopularityScore"))

# Guardar el DataFrame como un archivo de texto
popular_games_team.select("text_data").write.mode("overwrite").text("4_teamout.txt")
spark.stop()

