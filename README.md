PROJETO  SIMULANDO ARQUITETURA MEDALHÃO - (BRONZE-SILVER-GOLD)
ETAPAS:
![image](https://img.freepik.com/vetores-gratis/conjunto-de-medalhas-ilustracao_1284-11496.jpg?t=st=1729952877~exp=1729956477~hmac=982ca57afbecf13aa74c9e47f7a6374d9de07eec45004e93f4d9cb7651524cae&w=1060)

  1 -(RAW) Ingestão de dados consumidos da API IBGE Notícias (API pública, sem necessidade de token)  utilizando a biblioteca requests (método GET).
      Na primeira execução do código, é realizada a paginação de todos os dados da API até a última página que contenha dados para consumir.
      A partir da segunda execução, é feita uma verificação para identificar qual foi a última página inserida na camada RAW, e, a partir dessa página, 
      a ingestão continua a partir da última página já inserida.

   2 - (BRONZE) 
     Ingestão de dados da API Noticias no formato JSON na camada Bronze. Dados salvos no formato Delta (escrita append) e tabela criada no catalogo databricks.
     Nome tabela: bronze.ibge_news.

   3 - (SILVER)  
     Na camada Silver iniciaremos o preocesso de limpeza e deduplicação.
       
