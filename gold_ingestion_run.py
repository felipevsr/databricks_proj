from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pytz
from pyspark.sql.functions import lit
from pyspark.sql import Window
from pyspark.sql.functions import *
from datetime import datetime , timedelta

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
  # DeltaTable.isDeltaTable(spark, gold_delta_path)

  #### Nome  e caminho onde será feita a escrita da da tabela Delta  ##########
  db = 'gold'
  table = 'ibge_news'
  gold_delta_table = f'{db}.{table}'
  gold_delta_path = 'dbfs:/mnt/gold/'
  DT_END = datetime.now().date()
  DT_START = (DT_END - timedelta(days = 7))
  # if not DeltaTable.isDeltaTable(spark,gold_delta_path):
  #   DT_START = '2020-01-01'

  if DeltaTable.isDeltaTable(spark,gold_delta_path):
    DT_START = spark.sql("select cast(trunc(to_date(max(left(dateIngestion,10) ),'yyyy-MM-dd'),'MM') as string) from delta.`dbfs:/mnt/gold/` ").collect()[0][0]
  else:
    DT_START = spark.sql("select cast(trunc(to_date(min(left(dateIngestion,10 ) ),'yyyy-MM-dd'),'MM') as string) from delta.`dbfs:/mnt/silver/` ").collect()[0][0]

  ### Sistema de origem ####
  source_delta_table = 'silver.ibge_news'
  
  print("##----------------------------##")
  print(f"Data e Horário de execusão             ===>  {time_file} \n")
  print(f"DT_START                               ===>  {DT_START} ")
  print(f"DT_END                                 ===>  {DT_END}")
  print(f"Banco de Dados                         ===>  {db}")
  print(f"Nome da Tabela                         ===>  {table}")
  print(f"Tabela gold a ser criada             ===>  {gold_delta_table}")
  print(f"Caminho da tabela gold a ser criada  ===>  {gold_delta_path} \n")
  print("##----------------------------##")
  print(f"Tabela de Origem                     ===> {source_delta_table}")

except Exception as e:
  print(e)

class GoldIngestion:
  def __init__(self,source_delta_table,gold_delta_table,gold_delta_path,dtstart,dtend):
    self.source_delta_table = source_delta_table
    self.gold_delta_table = gold_delta_table
    self.gold_delta_path = gold_delta_path
    self.dtstart = dtstart
    self.dtend = dtend
    self.dt = str(dtend).replace('-','')[0:6]


  def create_structured_delta_schema(self):

    if not DeltaTable.isDeltaTable(spark,gold_delta_path):
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
                      StructField('product_id', StringType(),True,metadata={"comment": "ID do produto"}),
                      StructField('products', StringType(),True,metadata={"comment": "Produto"}),
                      StructField('relatedProducts', StringType(),True,metadata={"comment": "Produtos relacionados"}),
                      StructField('type', StringType(),True,metadata={"comment": "Tipo "}),
                      StructField('title', StringType(),True,metadata={"comment": "Título da Noticia"}),
                      StructField('dateIngestion', TimestampType(),True,metadata={"comment": "Data de Ingestão"}),
                      StructField('dt', StringType(),True,metadata={"comment": "Data para partiçao"})
                            ])
      ### Criando estrutura tabela Delta ###
      df =  spark.createDataFrame(data= [],schema=schema)
      print(f'Criando estrutura da tabela delta no caminho .. {gold_delta_path}')
      df.write.format('delta').partitionBy('dt').save(f'{gold_delta_path}')
      
      print(f'Tabela ===> {gold_delta_table}  ja foi anteriormente criada no caminho ===> {gold_delta_path}')
      ### Criando Data base  ###
      sql = f""" CREATE DATABASE IF NOT EXISTS {db} """
      spark.sql(sql)
      ### Criando tabela de metadadados db gold  ###
      sql =  f""" DROP TABLE IF EXISTS {gold_delta_table}"""
      spark.sql(sql)
      print(sql)

      sql =  f""" CREATE TABLE IF NOT EXISTS {gold_delta_table} USING DELTA LOCATION '{gold_delta_path}' """
      spark.sql(sql)
      print(sql,'\n')

      print(f'Tabela {gold_delta_table}  criada com sucesso !!!\n')
      
     
    else:
      print(f'Tabela ===> {gold_delta_table}  ja foi anteriormente criada no caminho ===> {gold_delta_path}')
      ### Criando Data base  ###
      sql = f""" CREATE DATABASE IF NOT EXISTS {db} """
      spark.sql(sql)
      ### Criando tabela de metadadados db gold  ###
      sql =  f""" DROP TABLE IF EXISTS {gold_delta_table}"""
      spark.sql(sql)
      print(sql)

      sql =  f""" CREATE TABLE IF NOT EXISTS {gold_delta_table} USING DELTA LOCATION '{gold_delta_path}' """
      spark.sql(sql)
      print(sql,'\n')

      print(f'Tabela {gold_delta_table}  criada com sucesso !!!\n')

  def campos_json(self):
    try:
      df_silver = spark.table(f'{self.source_delta_table}') ## Adicionar filtro
      ### Vericar com antecedencia na tabela silver campos com estrutura json ###
      list_columns = ['imagens']
      ### colocar um IF list_columns > 0 e else park.table(f'{self.source_delta_table}') 
      for column in df_silver.dtypes:
        if column[0] in list_columns:
          print(f'Criando estrutura JSON do campo == > {column[0]}')
          df_silver = df_silver.withColumn(f'{column[0]}_dict',from_json(f'{column[0]}',MapType(StringType(),StringType())))
      return  df_silver
    except:
      print('Não há campos para criar estrutura JSON')
      return df_silver = spark.table(f'{self.source_delta_table}') ## Adicionar filtro





    
  def save_gold(self):
    self.create_structured_delta_schema()



###### CONTINUAR #######

df_silver = spark.table("silver.ibge_news")
df_silver = df_silver.withColumn('images_dict',from_json(df_silver.imagens,MapType(StringType(),StringType())))

# SELECIONANDO O A LINHA COM MAIOR QTDE DE CAMPOS PARA APLICAR TRANSFORMAÇÃO
max_columns = df_silver.select(max(map_keys(col("images_dict"))) ).collect()[0][0]
# max_columns = list(max_columns)[0][0]
max_columns