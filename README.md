PROJETO  SIMULANDO ARQUITETURA MEDALHÃO - (BRONZE-SILVER-GOLD)
EATAPAS:

  1 -(RAW) Ingestão de dados consumidos da API IBGE Notícias utilizando a biblioteca requests (método GET).
      Na primeira execução do código, é realizada a paginação de todos os dados da API até a última página que contenha dados para consumir.
      A partir da segunda execução, é feita uma verificação para identificar qual foi a última página inserida na camada RAW, e, a partir dessa página, 
      a ingestão continua a partir da última página já inserida.

   2 - (BRONZE)   
