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
  # DeltaTable.isDeltaTable(spark, silver_delta_path)

  #### Nome  e caminho onde será feita a escrita da da tabela Delta  ##########
  db = 'silver'
  table = 'ibge_news'
  silver_delta_table = f'{db}.{table}'
  silver_delta_path = 'dbfs:/mnt/silver/'
  DT_END = datetime.now().date()
  DT_START = (DT_END - timedelta(days = 7))

  ### Sistema de origem ####
  source_delta_table = 'bronze.ibge_news'
  
  print("##----------------------------##")
  print(f"Data e Horário de execusão             ===>  {time_file} \n")
  print(f"DT_START                               ===>  {DT_START} ")
  print(f"DT_END                                 ===>  {DT_END}")
  print(f"Banco de Dados                         ===>  {db}")
  print(f"Nome da Tabela                         ===>  {table}")
  print(f"Tabela silver a ser criada             ===>  {silver_delta_table}")
  print(f"Caminho da tabela silver a ser criada  ===>  {silver_delta_path} \n")
  print("##----------------------------##")
  print(f"Tabela de Origem                     ===> {source_delta_table}")

except Exception as e:
  print(e)




class  SilverIngestion:
  def __init__(self,source_delta_table,silver_delta_table,silver_delta_path,dtstart,dtend):
    self.source_delta_table = source_delta_table
    self.silver_delta_table = silver_delta_table
    self.silver_delta_path = silver_delta_path
    self.dtstart = dtstart
    self.dtend = dtend

  def create_structured_delta_schema(self):

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
                      StructField('product_id', StringType(),True,metadata={"comment": "ID do produto"}),
                      StructField('products', StringType(),True,metadata={"comment": "Produto"}),
                      StructField('relatedProducts', StringType(),True,metadata={"comment": "Produtos relacionados"}),
                      StructField('type', StringType(),True,metadata={"comment": "Tipo "}),
                      StructField('title', StringType(),True,metadata={"comment": "Título da Noticia"}),
                      StructField('dateIngestion', TimestampType(),True,metadata={"comment": "Data de Ingestão"}),
                            ])
      ### Criando estrutura tabela Delta ###
      df =  spark.createDataFrame(data= [],schema=schema)
      print(f'Criando estrutura da tabela delta no caminho .. {silver_delta_path}')
      df.write.format('delta').partitionBy('referenceMonthDate').save(f'{silver_delta_path}')
      
      ### Criando Data base  ###
      sql = f""" CREATE DATABASE IF NOT EXISTS {db} """
      spark.sql(sql)
      print(sql,"\n")
      ### Criando tabela de metadadados db silver  ###
      sql =  f""" DROP TABLE IF EXISTS {silver_delta_table}"""
      spark.sql(sql)
      print(sql)

      sql =  f""" CREATE TABLE IF NOT EXISTS {silver_delta_table} USING DELTA LOCATION '{silver_delta_path}' """
      spark.sql(sql)
      print(sql,'\n')

      print(f'Tabela {silver_delta_table}  criada com sucesso !!!')

    else:
      print(f'Tabela ===> {silver_delta_table}  ja foi anteriormente criada no caminho ===> {silver_delta_path}')

  def silver_run(self):
    self.create_structured_delta_schema()

    df = spark.table(self.source_delta_table)
    ### função de janela para efetuar a deduplicaçao dos dados   #####
    row_numer_experssion = Window.partitionBy(col('id')).orderBy(col('DTPROC').desc())

    df_stage = (df.withColumn('referenceMonthDate',trunc(to_date(col('data_publicacao'),'dd/MM/yyyy HH:mm:ss').cast('date'),'MM'))
                  .withColumn('rownumber_wdw', row_number().over(row_numer_experssion))
         )
     
    df_stage = (df_stage.filter(col("rownumber_wdw") == 1)
                  .withColumn('publicationDate',
                             to_timestamp(col('data_publicacao'),'dd/MM/yyyy HH:mm:ss').cast('timestamp'))
                  .withColumn('newsHighlight', col('destaque').cast('string'))
                  .withColumn('editorials', col('editorias').cast('string'))
                  .withColumn('images', col('imagens').cast('string'))
                  .withColumn('introduction', col('introducao').cast('string'))
                  .withColumn('product_id', col('produto_id').cast('string'))
                  .withColumn('products', col('produtos').cast('string'))
                  .withColumn('relatedProducts', col('produtos_relacionados').cast('string'))
                  .withColumn('type', col('tipo').cast('string'))
                  .withColumn('title', col('titulo').cast('string'))
                  .withColumn('dateIngestion', lit(datetime.now() - timedelta(hours = 3)).cast('timestamp')) )
    
    df_final = (df_stage.select() 
                )

#### Continuar construindo   ####