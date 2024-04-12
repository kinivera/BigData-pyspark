import pyspark

# Inicializar SparkContext
sc = pyspark.SparkContext()

# Función para filtrar y limpiar datos de NASDAQ
def filter_nasdaq(line):
    try:
        fields = line.split(',')
        if len(fields) != 9:
            return False
        return True
    except:
        return False
        

# Función para filtrar y limpiar datos de la lista de empresas
def filter_company_list(line):
    try:
        fields = line.split('\t')
        if len(fields) != 5 or ("IPOyear" in line and "Symbol" in line):
            return False
        return True
    except:
        return False
      
def reduceByYear(nasdaq1,nasdaq2):
	if nasdaq1[0] > nasdaq2[0]:
		date_start = (nasdaq2[0], nasdaq2[1])
	else:
		date_start = (nasdaq1[0], nasdaq1[1])
		
	if nasdaq1[2] > nasdaq2[2]:
		date_end = (nasdaq1[2], nasdaq1[3])
	else:
		date_end = (nasdaq2[3], nasdaq2[3])
	return (*date_start, *date_end)
  
# Definir función para calcular el porcentaje de crecimiento
def growth_percentage(start, end):#precio apertura y cierre

	try:
		return "{:.2f}%".format(((float(start) - float(end)) / float(end)) * 100)
	except:
		return "N/A"


# Cargar archivos y filtrar datos
nasdaq = sc.textFile("/home/kinivera/BigData/Parcial2/input/NASDAQsample.csv")
company= sc.textFile("/home/kinivera/BigData/Parcial2/input/companylist.tsv")

nasdaq = nasdaq.filter(filter_nasdaq)
company = company.filter(filter_company_list)



# Mapear los datos de NASDAQ y la lista de empresas
nasdaq = nasdaq.map(lambda l: ((l.split(',')[2][:4],l.split(',')[1]), (l.split(',')[2], l.split(',')[3],l.split(',')[2], l.split(',')[6])))# (year, symbol), (date_start, start, date_end, end )
		 
#
nasdaq = nasdaq.reduceByKey(reduceByYear)
nasdaq = nasdaq.map(lambda l: (l[0][1], (l[0][0], growth_percentage(l[1][1], l[1][3]))))# symbol, (year, growth )

company= company.map(lambda l: (l.split("\t")[0], l.split("\t")[3]))  # symbol, sector

# Unir los RDDs por clave (símbolo)
joined_rdd = nasdaq.join(company)
joined_rdd = joined_rdd.sortByKey()

# Convertir los resultados a un formato adecuado para guardar como texto
joined_rdd = joined_rdd.map(lambda x: "{:^30}, {:^6}, {:^6}, {:^4}".format(x[1][1], x[1][0][0], x[0], x[1][0][1]))


# Guardar los resultados como un archivo de texto
joined_rdd = joined_rdd.coalesce(1)
joined_rdd.saveAsTextFile("2_out")
