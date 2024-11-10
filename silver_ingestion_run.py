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

'''
Essa configuração permite que o Delta Lake faça automaticamente a evolução do esquema 
ao detectar mudanças no esquema dos dados durante operações de merge, update ou append, 
sem a necessidade de redefinir manualmente o esquema.
'''
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
'''
Essa configuração permite que o Delta Lake faça automaticamente a união de arquivos pequenos 
durante operações de escrita, reduzindo a fragmentação sem a necessidade de otimizações manuais frequentes
'''
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")



try:
  time_file = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y%m%d_%H%M%S')
  # DeltaTable.isDeltaTable(spark, silver_delta_path)

  #### Nome  e caminho onde será feita a escrita da da tabela Delta  ##########
  db = 'silver'
  table = 'ibge_news'
  delta_table = f'{db}.{table}'
  silver_delta_path = 'dbfs:/mnt/silver/'

  ### Sistema de origem ####
  source_table = 'bronze.ibge_news'
  
  print("##----------------------------##")
  print(f"Data e Horário de execusão             ===> {time_file} \n")
  print(f"Banco de Dados                         ===>  {db}")
  print(f"Nome da Tabela                         ===>  {table}")
  print(f"Tabela silver a ser criada             ===>  {delta_table}")
  print(f"Caminho da tabela silver a ser criada  ===> {silver_delta_path} \n")
  print("##----------------------------##")
  print(f"Tabela de Origem                     ===> {source_table}")

except Exception as e:
  print(e)




if not DeltaTable.isDeltaTable(spark,silver_delta_path):
   print('Criando estrutura tabela Delta....\n')
   schema = StructType([

                  StructField('referenceMonthDate',DateType(),True,metadata={"comment": "Data do Mes de referencia"}),
                  StructField('id',LongType(),True,metadata={"comment": "Identificador único da notícia"}),
                  StructField('newsHighlight', StringType(),True,metadata={"comment": "Destaque"}),
                  StructField('editorials', StringType(),True,metadata={"comment": "Editorial"}),
                  StructField('images', StringType(),True,metadata={"comment": "Descrição das imagens"}),
                  StructField('publicationDate', TimestampType(),True,metadata={"comment": "Data da publicaçao"}),
                  StructField('introduction', StringType(),True,metadata={"comment": "Introduçao da Noticia"}),
                  StructField('link', StringType(),True,metadata={"comment": "Link da Noticia"}),
                  StructField('produto_id', StringType(),True,metadata={"comment": "ID do produto"}),
                  StructField('products', StringType(),True,metadata={"comment": "Produto"}),
                  StructField('relatedProducts', StringType(),True,metadata={"comment": "Produtos relacionados"}),
                  StructField('type', StringType(),True,metadata={"comment": "Tipo "}),
                  StructField('title', StringType(),True,metadata={"comment": "Título da Noticia"}),
                  StructField('dateIngestion', TimestampType(),True,metadata={"comment": "Data de Ingestão"}),
                        ])
   ### Criando estrutura tabela Delta ###
   df =  spark.createDataFrame(data= [],schema=schema)
   print(f'Criando estrutura da tabela delta no caminho .. {silver_delta_path}')
   df.write.format('delta').save(f'{silver_delta_path}')
   
   ### Criando Data base  ###
   sql = f""" CREATE DATABASE IF NOT EXISTS {db} """
   spark.sql(sql)
   print(sql,"\n")
   ### Criando tabela de metadadados db silver  ###
   sql =  f""" DROP TABLE IF EXISTS {delta_table}"""
   spark.sql(sql)
   print(sql)

   sql =  f""" CREATE TABLE IF NOT EXISTS {delta_table} USING DELTA LOCATION '{silver_delta_path}' """
   spark.sql(sql)
   print(sql,'\n')

   print(f'Tabela {delta_table}  criada com sucesso !!!')

else:
   print(f'Tabela ===> {delta_table}  ja foi anteriormente criada no caminho ===> {silver_delta_path}')

   


#### Continuar construindo   ####