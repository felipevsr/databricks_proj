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
  historic_path = 'dbfs:/mnt/historic/bronze/'
  
  print("##----------------------------##")
  print(f"Data e Horário de execusão             ===> {time_file} \n")
  print(f"Tabela bronze a ser criada             ===>  {delta_table}")
  print(f"Caminho da tabela Bronze a ser criada  ===> {bronze_delta_path} \n")
  print("##----------------------------##")
  print(f"Sistema de origem                      ===> {source_system_path}")
  print(f"Caminho para gravação do histórico     ===> {historic_path}")
except Exception as e:
  print(e)



class BronzeIngestion:

  def __init__(self,db,table,bronze_delta_path,source_system_path,historic_path):
    self.db                 = db
    self.table              = table
    self.delta_table        = f"{self.db}.{self.table}"
    self.bronze_delta_path  = bronze_delta_path
    self.source_system_path = source_system_path
    self.historic_path      = historic_path

  ### Listando arquivos camada RAW   ####
  def list_all_files(self):
    try:
      ### self.source_system_path => sistema de origem   ####
      all_files = []
      for files in dbutils.fs.ls(self.source_system_path):
        all_files.append([files.path,files.path.split("_")[-1].replace("/","")])
      all_files.sort(key = lambda pos:pos[1])
      # print(all_files)
      return [all_files[i][0] for i in range(0,len(all_files))]

    except Exception as error:
      print(f"{error}") 


  
  def separate_files(self):
    try:
      ### Separando arquivos a cada 10 items ou a quantidade que for..
      ### caso seja menor do que 10
      all_files = self.list_all_files()
      return  [ all_files[i:i+10] for i in range(0,len(all_files),10)]
    
    except Exception as error:
      print(f"{error}") 
  
  ### Salvando Dados na camada Bronze ###
  def save_files_delta(self):
    try:
      print(" Inicinado método list_all_files().... \n Inicinado método list_all_files() .... \n Inicinado método save_files_delta() ....\n")
      files_to_record = self.separate_files()
      for files in files_to_record:
        # print(files,len(files),"\n")
        df =spark.read.json(files)
        #### Adicionando data de ingestao ####
        df = df.withColumn('DTPROC',lit(datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y%m%d_%H%M%S')))
        print(f" Salvando dados da API no caminho ===> {self.bronze_delta_path}  com o nome ===>  {self.delta_table} ")
        df.write.mode('append').format('delta').save(self.bronze_delta_path)
      print("\n Dados salvos em formato delta. \n")
    except Exception as error:
      print(f"{error}")

  ##### Copiando arquivos para Historico  ####
  def copy_to_historic(self):
    try:
      print(f"\n Copiando arquivos de ==> {self.source_system_path} para ==>  {self.historic_path} \n ")
      dbutils.fs.cp(self.source_system_path,self.historic_path,recurse = True)
      print(" Copia realizada com sucesso.\n")
    except Exception as error:
      print(f"{error}")
  
  
  ### Criando tabela no hive_metastore (catalogo Databricks)  ####
  def create_delta_table_hive(self):
    try:
      sql = f""" CREATE DATABASE IF NOT EXISTS {self.db} """
      spark.sql(sql)
      print(sql,"\n")

      sql_drop = f""" DROP TABLE IF EXISTS {self.delta_table}  """
      spark.sql(sql_drop)
      print(sql_drop,"\n") 

      sql_table = f""" CREATE TABLE IF NOT EXISTS {self.delta_table} USING DELTA LOCATION '{self.bronze_delta_path}' """
      spark.sql(sql_table)
      print(sql_table)

    except Exception as error:
      print(f"{error}")

  

  #### Executando  ####
  def bronze_run(self):
    print("Inicio do processo de ingestão na bronze... \n")
    # self.list_all_files()
    # self.separate_files()
    self.save_files_delta()
    self.copy_to_historic()
    self.create_delta_table_hive()
    print("Processo finalizado!..")
    
    
############################################################
bronze = BronzeIngestion('bronze','ibge_news','dbfs:/mnt/bronze/','dbfs:/mnt/raw_3/','dbfs:/mnt/historic/bronze/')
bronze.bronze_run()


#### Ajustar copy to historic para verifiar apenas arquivos novos  ######