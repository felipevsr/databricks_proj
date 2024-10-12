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



### Listando arquivos camada RAW
def list_all_files(path_files):

  all_files = []
  for files in dbutils.fs.ls(path_files):
    all_files.append([files.path,files.path.split("_")[-1].replace("/","")])
  all_files.sort(key = lambda pos:pos[1])
  # print(all_files)
  return [all_files[i][0] for i in range(0,len(all_files))]

### Separando Arquivos de 10 em 10  ####

def separate_files():
  # separate_files = []
  all_files = list_all_files(source_system_path)
  return  [ all_files[i:i+10] for i in range(0,len(all_files),10)]


# Gravando Arquivo na Tabela Bronze (delta Table)(
def save_files_delta():
  files_to_record = separate_files()
  for files in files_to_record:
    # print(files,len(files),"\n")
    df =spark.read.json(files)
    #### Adicionando data de ingestao ####
    df = df.withColumn('DTPROC',lit(datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y%m%d_%H%M%S')))
    print(f"salvando tabela bronze no caminho ===> {bronze_delta_path}  com o nome ===>  {delta_table} ")
    df.write.mode('append').format('delta').save(bronze_delta_path)


############# CONTINUAR  AJUSTES  ###########   (adicionar criação de tabela no hive_metastore try except e criar CLASSE)