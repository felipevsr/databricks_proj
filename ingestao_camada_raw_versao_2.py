from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
import datetime, pytz
from pyspark.sql.functions import lit
from pyspark.sql import Window
from pyspark.sql.functions import *

import requests
import json

try:
  valor_maximo_raw = [int(i.name.split('_')[1].replace("/","")) for i in dbutils.fs.ls('dbfs:/mnt/raw_2/')]
  valor_maximo_raw.sort()
  valor_maximo_raw = valor_maximo_raw[-1]
  ## Verificando numero total de paginas ##
  content_json = requests.get(f"http://servicodados.ibge.gov.br/api/v3/noticias/?page={1}")
  content_json = content_json.json()
  print("Ultima pagina inserida na RAw ==>  " ,valor_maximo_raw)
  acumulo_paginas = []
  contador = valor_maximo_raw
  if valor_maximo_raw < content_json["totalPages"]:

    print(f"Iniciando Proceeso a partit da pagina ==> {contador}")
    while contador <= content_json["totalPages"]:
      noticias_API = requests.get(f"http://servicodados.ibge.gov.br/api/v3/noticias/?page={contador}")
      noticias_json = noticias_API.json()
      acumulo_paginas.append(noticias_json["items"])

      if noticias_json["items"] != [] and str(noticias_json["page"])[-1] == "0":
        #  print(f"Guardando pagina  {contador}")
        #  noticias.append(noticias_json["items"])
        ### Ajuntando todas as paginas appendadas em uma lista unica ####
        result_acumulo_paginas =[acumulo_paginas[i][item] for i in range(0,len(acumulo_paginas))
                for item in range(0,len(acumulo_paginas[i]))]   
        df = spark.createDataFrame(result_acumulo_paginas).withColumn(f'to_the_page',lit(contador))
        print(f"\t Gravando até a pagina {contador}  no diretorio dbfs dbfs:/mnt/raw_2/")
        df.write.mode("overwrite").json(f'dbfs:/mnt/raw_2/filetothepage_{contador}')
        acumulo_paginas = []
      elif  noticias_json["page"] == content_json["totalPages"]:
        ### Ajuntando todas as paginas appendadas em uma lista unica ####
        result_acumulo_paginas =[acumulo_paginas[i][item] for i in range(0,len(acumulo_paginas))
                for item in range(0,len(acumulo_paginas[i]))]  
        df = spark.createDataFrame(result_acumulo_paginas).withColumn(f'to_the_page',lit(contador))
        print(f"\t Gravando a pagina {contador}  no diretorio dbfs dbfs:/mnt/raw/")
        df.write.mode("overwrite").json(f'dbfs:/mnt/raw_2/filetothepage_{contador}')
        acumulo_paginas = []

      contador = contador +1
    print("Processo finalizado")
  else:
    print("Todas as paginas da API ja foram inseridas na camada RAW")

except:
  print("Nao possui arquivos")
  print("Buscando noticias desde a pagina 1")
  content_json = requests.get(f"http://servicodados.ibge.gov.br/api/v3/noticias/?page={1}")
  content_json = content_json.json()
  acumulo_paginas = []
  contador = 1
  while contador <= content_json["totalPages"]:
    noticias_API = requests.get(f"http://servicodados.ibge.gov.br/api/v3/noticias/?page={contador}")
    noticias_json = noticias_API.json()
    acumulo_paginas.append(noticias_json["items"])

    if noticias_json["items"] != [] and str(noticias_json["page"])[-1] == "0":
      #  print(f"Guardando pagina  {contador}")
      #  noticias.append(noticias_json["items"])
      ### Ajuntando todas as paginas appendadas em uma lista unica ####
      result_acumulo_paginas =[acumulo_paginas[i][item] for i in range(0,len(acumulo_paginas))
                for item in range(0,len(acumulo_paginas[i]))]   
      
      df = spark.createDataFrame(result_acumulo_paginas).withColumn(f'to_the_page',lit(contador))
      print(f"\t Gravando até a pagina {contador}  no diretorio dbfs dbfs:/mnt/raw_2/")
      df.write.mode("overwrite").json(f'dbfs:/mnt/raw_2/filetothepage_{contador}')
      acumulo_paginas = []
    elif noticias_json["page"] == content_json["totalPages"]:
      print(f"Gravando todas as paginas até {contador}  no diretorio dbfs dbfs:/mnt/raw_2/")
      ### Ajuntando todas as paginas appendadas em uma lista unica ####
      result_acumulo_paginas =[acumulo_paginas[i][item] for i in range(0,len(acumulo_paginas))
                for item in range(0,len(acumulo_paginas[i]))]  
      
      df = spark.createDataFrame(result_acumulo_paginas).withColumn(f'to_the_page',lit(contador))
      print(f"\t Gravando a pagina {contador}  no diretorio dbfs dbfs:/mnt/raw_2/")
      df.write.mode("overwrite").json(f'dbfs:/mnt/raw_2/filetothepage_{contador}')
      acumulo_paginas = []
    contador = contador +1
  print("Processo finalizado")


# df2 = spark.read.json('dbfs:/mnt/raw_2/*')
df2.printSchema()