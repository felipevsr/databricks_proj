PROJETO  SIMULANDO ARQUITETURA MEDALHÃO - (BRONZE-SILVER-GOLD)
ETAPAS:

  1 -(RAW) Ingestão de dados consumidos da API IBGE Notícias (API pública, sem necessidade de token)  utilizando a biblioteca requests (método GET).
      Na primeira execução do código, é realizada a paginação de todos os dados da API até a última página que contenha dados para consumir.
      A partir da segunda execução, é feita uma verificação para identificar qual foi a última página inserida na camada RAW, e, a partir dessa página, 
      a ingestão continua a partir da última página já inserida.

   2 - (BRONZE) 
     Ingestão de dados da API Noticias no formato JSON na camada Bronze. Dados salvos no formato Delta (escrita append) e tabela criada no catalogo databricks.Nome tabela: bronze.ibge_news
     ![image](https://github.com/user-attachments/assets/0505e64d-83f3-4336-9cdb-5efbfefb0505)
