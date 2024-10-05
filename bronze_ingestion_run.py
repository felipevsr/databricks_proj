##### IMPORTS   #####

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


############ VARIAVEIS   ############
try:
  timezone_sp = pytz.timezone('America/Sao_Paulo')
  current_time = datetime.now(timezone_sp)
  time_file = datetime.strftime(current_time,'%Y%m%d-%H%M%S')

  #### Nome  e caminho onde será feita a escrita da da tabela Delta  ##########
  db = 'bronze.'
  table = 'ibge_news'
  delta_table = f'{db}{table}'
  bronze_delta_path = 'dbfs:/mnt/bronze/'

  ### Sistema de origem ####
  source_system_path = 'dbfs:/mnt/raw_3/'

  ### caminho para gravação do histórico ###
  historic_path = 'dbfs:/mnt/historic/'
  
  print("##----------------------------##")
  print(f"Data e Horário de execusão             ===> {time_file} \n")
  print(f"Tabela bronze a ser criada             ===>  {delta_table}")
  print(f"Caminho da tabela Bronze a ser criada  ===> {bronze_delta_path} \n")
  print("##----------------------------##")
  print(f"Sistema de origem                      ===> {source_system_path}")
  print(f"Caminho para gravação do histórico     ===> {historic_path}")
except Exception as e:
  print(e)




files = []
for i in range(0,11):
  files.append(dbutils.fs.ls('dbfs:/mnt/raw/')[i].path)

### é Possivel fazer a Leitura de varios arquivos de forma Multipla, passando a referencia deles dentro de uma lista  ##########

spark.read.json(files).display()

#### CONTINUAR APOS NORMALIZAÇÂO DA API IBGE NOTICIAS  ########