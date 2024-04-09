import pyspark

sc = pyspark.SparkContext()

def NASDAQ(line):
    try:
        fields = line.split(',')
        if len(fields) != 9:
            return False
        #int(fields[2][:4])
        return True

    except:
        return False

def COMPANYLIST(line):
    try:
        fields = line.split('\t')
        if len(fields) != 5 or ("IPOyear" in line and "Symbol" in line):
            return False
        
        return True

    except:
        return False

#Cargar archivos y limpiar
nasdaq = sc.textFile("/home/kinivera/BigData/Parcial2/input/NASDAQsample.csv")
companylist = sc.textFile("/home/kinivera/BigData/Parcial2/input/companylist.tsv")

nasdaq = nasdaq.filter(NASDAQ)
companylist = companylist.filter(COMPANYLIST)

nasdaq = nasdaq.map(lambda l: (l.split(',')[1], (l.split(',')[2][:4], int(l.split(',')[7])))) #symbol,(date,n)
companylist = companylist.map(lambda l: (l.split("\t")[0], l.split("\t")[3])) #symbol,sector

joined_rdd = nasdaq.join(companylist) #symbol,((date,n),sector)

#print(joined_rdd.take(10))

features = joined_rdd.map(lambda row: ((row[1][1], row[1][0][0]), row[1][0][1])) #(sector,date),n

# Reducir por clave (Año, Sector) sumando el número de operaciones
sector_counts = features.reduceByKey(lambda x, y: x + y)#[((sector,año),n),....]

#print(sector_counts.take(10))

# Encontrar el sector con el mayor número de operaciones para cada año
max_sector_per_year = sector_counts.map(lambda x: (x[0][1], (x[1],x[0][0]))) #[(año,(n,sector)),....]

result = max_sector_per_year.reduceByKey(lambda x, y: x if x[0] > y[0] else y) #[(year,(n mayor,sector))]

#print(result.take(10))

#ordenar x year
result = result.sortByKey()

# Convertir el RDD a un formato adecuado para guardar como texto
max_sector_per_year_formatted = result.map(lambda x: (x[1][1], "{}, {}".format(x[0], x[1][0])))




# Guardar el RDD como un archivo de texto
max_sector_per_year_formatted = max_sector_per_year_formatted.coalesce(1)
max_sector_per_year_formatted.saveAsTextFile("1_out")




