from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
import datetime, pytz
from pyspark.sql.functions import lit
from pyspark.sql import Window
from pyspark.sql.functions import *

import requests
import json




# Requisição da API de noticias IBGE
content_json = requests.get("http://servicodados.ibge.gov.br/api/v3/noticias/")
content_json = content_json.json()

### ###  Ingestão pagina por pagina na camada raw DBFS  'dbfs:/mnt/raw/'' #####
try:
  valor_maximo_raw = [int(i.name.split('_')[1].replace("/","")) for i in dbutils.fs.ls('dbfs:/mnt/raw/')]
  valor_maximo_raw.sort()
  valor_maximo_raw = valor_maximo_raw[-1]
  print("Ultima pagina inserida na RAw ==>  " ,valor_maximo_raw)
  contador = valor_maximo_raw
  if valor_maximo_raw < content_json["totalPages"]:

    print(f"Iniciando Procseso a partir da pagina ==> {contador}")
    while contador <= content_json["totalPages"]:
      
      noticias_API = requests.get(f"http://servicodados.ibge.gov.br/api/v3/noticias/?page={contador}")
      noticias_json = noticias_API.json()
      if noticias_json["items"] != []:
        #  print(f"Guardando pagina  {contador}")
        #  noticias.append(noticias_json["items"])
        df = spark.createDataFrame(noticias_json["items"]).withColumn(f'pagina',lit(contador))
        # print(f"\t Gravando a pagina {contador}  no diretorio dbfs dbfs:/mnt/raw/")
        df.write.mode("overwrite").json(f'dbfs:/mnt/raw/file_{contador}')
        ## Adicionar logica para ggravar em um Dataframe
        contador = contador +1
      if str(contador)[-1] == "0" or contador == content_json["totalPages"]: print(f"Gravando no diretorio dbfs dbfs:/mnt/raw/ até a pagina {contador}")
  else:
    print("Todas as paginas da API ja foram inseridas na camada RAW")
  print("Processo finalizado")

except Exception as e:
  print("Nao possui arquivos")
  print("Buscando noticias dese a pagina 1")
  contador = 1
  while contador <= content_json["totalPages"]:
     noticias_API = requests.get(f"http://servicodados.ibge.gov.br/api/v3/noticias/?page={contador}")
     noticias_json = noticias_API.json()
     if noticias_json["items"] != []:
      #  print(f"Inserindo pagina na raw ==>    {contador}")
      #  noticias.append(noticias_json["items"])
       df = spark.createDataFrame(noticias_json["items"]).withColumn(f'pagina',lit(contador))
      #  print(f"\t Granvando a pagina {contador}  no diretorio dbfs dbfs:/mnt/raw/")
       df.write.mode("overwrite").json(f'dbfs:/mnt/raw/file_{contador}')
       contador = contador +1
     if str(contador)[-1] == "0" or contador == content_json["totalPages"]: print(f"Gravando no diretorio dbfs dbfs:/mnt/raw/ até a pagina {contador}")
  print("Processo finalizado")


### Verificando dados da API gravados na pasta raw ###
display(dbutils.fs.ls('dbfs:/mnt/raw'))