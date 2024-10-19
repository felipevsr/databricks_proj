from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pytz
from pyspark.sql.functions import lit
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime

import requests
import json

class RawLayerIngestion:

  def __init__(self,url,first_page,raw_directory):
    self.URL_API = url
    self.first_page = first_page
    self.raw_directory = raw_directory

  def raw_ingestion(self):
    try:
      ## Verifica a pasta data mais recente ###
      lst_folder_date = [lst_date.name.replace('/','') for lst_date in dbutils.fs.ls(f'{self.raw_directory}')]
      lst_folder_date.sort()
      ### Verifica a pasta de ingestao da API de dados mais recente  ###
      valor_maximo_raw =[int(i.name.split("_")[3]) for i in dbutils.fs.ls(f'{self.raw_directory}{lst_folder_date[-1]}/')]
      valor_maximo_raw.sort()
      valor_maximo_raw = valor_maximo_raw[-1]
      ## Verificando numero total de paginas ##
      content_json = requests.get("{url}?page={page}".format(url=self.URL_API,page=self.first_page))
      content_json = content_json.json()
      print("Ultima pagina inserida na RAw ==>  " ,valor_maximo_raw)
      acumulo_paginas = []
      np_acumulo = []
      contador = valor_maximo_raw
      if valor_maximo_raw <= content_json["totalPages"]:

        print(f"Iniciando Proceeso a partit da pagina ==> {contador}")
        while contador <= content_json["totalPages"]:
          noticias_API = requests.get("{url}?page={contador}".format(url=self.URL_API,contador=contador))
          noticias_json = noticias_API.json()
          acumulo_paginas.append(noticias_json["items"])
          np_acumulo.append(noticias_json['page'])
          ##### Data e hora de ingestao na camada RAW   ####
          time_file = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y%m%d_%H%M%S')

          if noticias_json["items"] != [] and str(noticias_json["page"])[-1] == "0":

            result_acumulo_paginas =[acumulo_paginas[i][item] for i in range(0,len(acumulo_paginas))
                                                                for item in range(0,len(acumulo_paginas[i]))]   
            df = spark.createDataFrame(result_acumulo_paginas).withColumn(f'PAGE',lit(f"{np_acumulo[0]} - to - {np_acumulo[-1]} - {time_file}"))
            print(f"\t Gravando até a pagina {contador}  no diretorio dbfs {self.raw_directory}{time_file[0:6]}/")
            df.write.mode("overwrite").json(f'{self.raw_directory}{time_file[0:6]}/ibgeapipage_{np_acumulo[0]}_to_{np_acumulo[-1]}_{time_file}')
            acumulo_paginas = []
            np_acumulo = []
          elif  noticias_json["page"] == content_json["totalPages"]:
            ### Ajuntando todas as paginas appendadas em uma lista unica ####
            result_acumulo_paginas =[acumulo_paginas[i][item] for i in range(0,len(acumulo_paginas))
                                                                    for item in range(0,len(acumulo_paginas[i]))]  
            df = spark.createDataFrame(result_acumulo_paginas).withColumn(f'PAGE',lit(f"{np_acumulo[0]} - to - {np_acumulo[-1]} - {time_file}"))
            print(f"\t Gravando a pagina {contador}  no diretorio dbfs {self.raw_directory}{time_file[0:6]}/")
            df.write.mode("overwrite").json(f'{self.raw_directory}{time_file[0:6]}/ibgeapipage_{np_acumulo[0]}_to_{np_acumulo[-1]}_{time_file}')
            acumulo_paginas = []
            np_acumulo = []

          contador = contador +1
        print("Processo finalizado")
      else:
        print("Todas as paginas da API ja foram inseridas na camada RAW")

    except:
      try:
        print("Primeira ingestão ou Nao possui arquivos na Data corrente")
        print("Buscando noticias desde a pagina ....")
        noticias_API = requests.get("{url}?page={page}".format(url=self.URL_API,page=self.first_page))
        noticias_json = noticias_API.json()
        acumulo_paginas = []
        np_acumulo = []

        try:
                ## Verifica a pasta data mais recente ###
          lst_folder_date = [lst_date.name.replace('/','') for lst_date in dbutils.fs.ls(f'{self.raw_directory}')]
          lst_folder_date.sort()
          ### Verifica a pasta de ingestao da API de dados mais recente para adicionar ao contador  ###
          contador =[int(i.name.split("_")[3]) for i in dbutils.fs.ls(f'{self.raw_directory}{lst_folder_date[-1]}/')]
          contador.sort()
          contador = contador[-1]
        except:
          contador = 1

        while contador <= noticias_json["totalPages"]:
          noticias_API = requests.get("{url}?page={contador}".format(url=self.URL_API,contador=contador))
          noticias_json = noticias_API.json()
          acumulo_paginas.append(noticias_json["items"])
          np_acumulo.append(noticias_json['page'])
          ##### Data e hora de ingestao na camada RAW   ####
          time_file = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y%m%d_%H%M%S')

          if noticias_json["items"] != [] and str(noticias_json["page"])[-1] == "0":

            result_acumulo_paginas =[acumulo_paginas[i][item] for i in range(0,len(acumulo_paginas))
                                                                for item in range(0,len(acumulo_paginas[i]))]   
            
            df = spark.createDataFrame(result_acumulo_paginas).withColumn(f'PAGE',lit(f"{np_acumulo[0]} - to - {np_acumulo[-1]} - {time_file}"))
            print(f"\t Gravando até a pagina {contador}  no diretorio dbfs {self.raw_directory}{time_file[0:6]}/")
            df.write.mode("overwrite").json(f'{self.raw_directory}{time_file[0:6]}/ibgeapipage_{np_acumulo[0]}_to_{np_acumulo[-1]}_{time_file}')
            acumulo_paginas = []
            np_acumulo = []
          elif noticias_json["page"] == noticias_json["totalPages"]:
            print(f"Gravando todas as paginas até {contador}  no diretorio dbfs {self.raw_directory}{time_file[0:6]}/")
            ### Ajuntando todas as paginas appendadas em uma lista unica ####
            result_acumulo_paginas =[acumulo_paginas[i][item] for i in range(0,len(acumulo_paginas))
                                                                  for item in range(0,len(acumulo_paginas[i]))]  
            
            df = spark.createDataFrame(result_acumulo_paginas).withColumn(f'PAGE',lit(f"{np_acumulo[0]} - to - {np_acumulo[-1]} - {time_file}"))
            df.write.mode("overwrite").json(f'{self.raw_directory}{time_file[0:6]}/ibgeapipage_{np_acumulo[0]}_to_{np_acumulo[-1]}_{time_file}')
            acumulo_paginas = []
            np_acumulo = []
          contador = contador +1
        print("Processo finalizado")
      except Exception as e:
        print(f"===>>>> {e}")

  def start_run(self):
    self.raw_ingestion()
  




### API IBGE de noticias, iniciando da pagina 1,  diretório de armazenamento ###
ingestao_raw = RawLayerIngestion('http://servicodados.ibge.gov.br/api/v3/noticias/',1,'dbfs:/mnt/raw_3/')
ingestao_raw.start_run()