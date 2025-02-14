PROJETO  SIMULANDO ARQUITETURA MEDALHÃO UTILIZANDO DATABRICKS COMMUNITY - (BRONZE-SILVER-GOLD)
ETAPAS:
![image](https://github.com/user-attachments/assets/8632e55c-48ee-499d-b580-c8b5bedc1546)



  1 -(RAW) Ingestão de dados consumidos da API IBGE Notícias (API pública, sem necessidade de token)  utilizando a biblioteca requests (método GET).
      Na primeira execução do código, é realizada a paginação de todos os dados da API até a última página que contenha dados para consumir.
      A partir da segunda execução, é feita uma verificação para identificar qual foi a última página inserida na camada RAW, e, a partir dessa página, 
      a ingestão continua a partir da última página já inserida.

   2 - (BRONZE) 
     Ingestão de dados da API Noticias no formato JSON na camada Bronze. Dados salvos no formato Delta (escrita append) e tabela criada no catalogo databricks.
     Nome tabela: bronze.ibge_news.

   3 - (SILVER)  
     Na camada Silver, criamos a estrutura da tabela Delta, iniciamos o processo de limpeza e deduplicação, e aplicamos filtros por data. Por fim, particionamos e realizamos a escrita da tabela utilizando MERGE (upsert) para inserir e atualizar os dados de forma eficiente.
     
 4 - (GOLD)   
     Na camada Gold, como não havia outras tabelas para realizar junções e agregações, foi feita apenas a separação de campos que estavam aninhados em um único campo JSON em vários novos campos individuais. Por fim, a tabela resultante foi salva no formato Delta, já atualizada.

       
