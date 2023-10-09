# Databricks notebook source
# DBTITLE 1,Importar Bibliotecas 
from pyspark.sql.functions import desc, asc 
from pyspark.sql.functions import when
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# COMMAND ----------

# DBTITLE 1,Conection
spark.conf.set(
    "fs.azure.account.key.saneobankaj.dfs.core.windows.net",
    "PpLTVTrybFIV1EE6XUcfCZtizzaQvQfgyJVhz7smNoyZEdyMK8J3Ea/iptG1YTmRkCg3d1lNKGI1+AStXY99dw==")

# COMMAND ----------

# DBTITLE 1,Carregando DataFrame's
df_src_NeoBank = spark.read.csv("abfss://bronze@saneobankaj.dfs.core.windows.net/NeoBank_Modelling_Source.csv", header = True)
df_dest_NeoBank = spark.read.csv("abfss://gold@saneobankaj.dfs.core.windows.net/NeoBank_Modelling.csv", header = True)

display(df_src_NeoBank)
display(df_dest_NeoBank)

# COMMAND ----------

# DBTITLE 1,aplicando filtro de ativos IsActiveMember
df_src_NeoBank = df_src_NeoBank.filter(df_src_NeoBank.IsActiveMember == 1)

display(df_src_NeoBank)

# COMMAND ----------

# DBTITLE 1,renomear colunas
##Renomeando origem

df_dest_NeoBank = df_dest_NeoBank.withColumnRenamed('RowNumber', 'RowNumber_dest') \
                    .withColumnRenamed('Customerid','costumerid_dest') \
                    .withColumnRenamed('Surname', 'Surname_dest') \
                    .withColumnRenamed('CreditScore', 'CreditScore_dest') \
                    .withColumnRenamed('Geography', 'Geography_dest') \
                    .withColumnRenamed('Gender', 'Gender_dest') \
                    .withColumnRenamed('Age', 'Age_dest') \
                    .withColumnRenamed('Tenure', 'Tenure_dest') \
                    .withColumnRenamed('Balance', 'Balance_dest') \
                    .withColumnRenamed('NumOfProducts', 'NumOfProducts_dest') \
                    .withColumnRenamed('HasCrCard', 'HasCrCard_dest') \
                    .withColumnRenamed('IsActiveMember', 'IsActiveMember_dest') \
                    .withColumnRenamed('EstimatedSalary', 'EstimatedSalary_dest') \
                    .withColumnRenamed('Exited' , 'Exited_dest')

df_src_NeoBank= df_src_NeoBank.withColumnRenamed('RowNumber', 'RowNumber_src') \
                    .withColumnRenamed('Customerid','costumerid_src') \
                    .withColumnRenamed('Surname', 'Surname_src') \
                    .withColumnRenamed('CreditScore', 'CreditScore_src') \
                    .withColumnRenamed('Geography', 'Geography_src') \
                    .withColumnRenamed('Gender', 'Gender_src') \
                    .withColumnRenamed('Age', 'Age_src') \
                    .withColumnRenamed('Tenure', 'Tenure_src') \
                    .withColumnRenamed('Balance', 'Balance_src') \
                    .withColumnRenamed('NumOfProducts', 'NumOfProducts_src') \
                    .withColumnRenamed('HasCrCard', 'HasCrCard_src') \
                    .withColumnRenamed('IsActiveMember', 'IsActiveMember_src') \
                    .withColumnRenamed('EstimatedSalary', 'EstimatedSalary_src') \
                    .withColumnRenamed('Exited' , 'Exited_src')

display(df_src_NeoBank)
display(df_dest_NeoBank)

# COMMAND ----------

# DBTITLE 1,left Join src X dest
df_client_join = df_src_NeoBank.join(df_dest_NeoBank, df_src_NeoBank.costumerid_src == df_dest_NeoBank.costumerid_dest, 'left' )

display(df_client_join)

# COMMAND ----------

# DBTITLE 1,New client
df_new_client = df_client_join.where('costumerid_dest is null') 

display(df_new_client)

# COMMAND ----------

# DBTITLE 1,selecionando colunas 
df_new_client = df_new_client.select('RowNumber_src',
                                    'costumerid_src',
                                    'Surname_src',
                                    'CreditScore_src',
                                    'Geography_src',
                                    'Gender_src',
                                    'Age_src',
                                    'Tenure_src',
                                    'Balance_src',
                                    'NumOfProducts_src',
                                    'HasCrCard_src',
                                    'IsActiveMember_src',
                                    'EstimatedSalary_src',
                                    'Exited_src')
display(df_new_client)

# COMMAND ----------

# DBTITLE 1,update
df_update = df_client_join.where('(costumerid_src == costumerid_dest)'
                                'and (Surname_src != Surname_dest)'
                                'or (CreditScore_src != CreditScore_dest)'
                                'or (Geography_src != Geography_dest)'
                                'or (Gender_src != Gender_dest)'
                                'or (Age_src != Age_dest)'
                                'or (Tenure_src != Tenure_dest)'
                                'or (Balance_src != Balance_dest)'
                                'or (NumOfProducts_src != NumOfProducts_dest)'
                                'or (HasCrCard_src != HasCrCard_dest)'
                                'or (IsActiveMember_src != IsActiveMember_dest)'
                                'or (EstimatedSalary_src != EstimatedSalary_dest)'
                                'or (Exited_src != Exited_dest)')

df_update_inativos = df_update

df_update = df_update.select('RowNumber_src',
                                    'costumerid_src',
                                    'Surname_src',
                                    'CreditScore_src',
                                    'Geography_src',
                                    'Gender_src',
                                    'Age_src',
                                    'Tenure_src',
                                    'Balance_src',
                                    'NumOfProducts_src',
                                    'HasCrCard_src',
                                    'IsActiveMember_src',
                                    'EstimatedSalary_src',
                                    'Exited_src')

df_update_inativos = df_update_inativos.select('RowNumber_dest',
                                'costumerid_dest',
                                'Surname_dest',
                                'CreditScore_dest',
                                'Geography_dest',
                                'Gender_dest',
                                'Age_dest',
                                'Tenure_dest',
                                'Balance_dest',
                                'NumOfProducts_dest',
                                'HasCrCard_dest',
                                'IsActiveMember_dest',
                                'EstimatedSalary_dest',
                                'Exited_dest')

display(df_update)
display(df_update_inativos)

# COMMAND ----------

# DBTITLE 1,Inativando registro 
df_update_inativos = df_update_inativos.select('*').withColumn('IsActiveMember_dest', when(df_update_inativos.IsActiveMember_dest == '1', '0').otherwise(df_update_inativos.IsActiveMember_dest))

display(df_update_inativos)


# COMMAND ----------

# DBTITLE 1,visao geral update, novo registro e inativo
display(df_new_client)
display(df_update)
display(df_update_inativos)

# COMMAND ----------

df_merge = df_dest_NeoBank.unionAll(df_update_inativos)

df_merge.where('costumerid_dest == 15628319').display()

# COMMAND ----------

df_merge = df_merge.withColumn('_row_number', row_number().over(Window.partitionBy('costumerid_dest').orderBy('IsActiveMember_dest')))

##display(df_merge)


df_merge.select('*').where('costumerid_dest == 15628319').display()


# COMMAND ----------

# DBTITLE 1,Remover Duplicado
dfmerge = df_merge.select('*').where('_row_number == 1')
display(dfmerge)


# COMMAND ----------

dfmerge.select('*').where('costumerid_dest == 15628319').display() 

# COMMAND ----------

# DBTITLE 1,Remover coluna auxiliar
dfmerge = dfmerge.drop('_row_number')

# COMMAND ----------

df_client_final = dfmerge.unionAll(df_new_client).unionAll(df_update)

df_client_final.select('*').where('costumerid_dest == 15628319 or costumerid_dest == 215628319').display()

# COMMAND ----------

# DBTITLE 1,Renomeando coluna dfFinal
df_client_final = df_client_final.withColumnRenamed('RowNumber_dest', 'RowNumber') \
                    .withColumnRenamed('costumerid_dest', 'Customerid') \
                    .withColumnRenamed('Surname_dest', 'Surname') \
                    .withColumnRenamed('CreditScore_dest', 'CreditScore') \
                    .withColumnRenamed('Geography_dest', 'Geography') \
                    .withColumnRenamed('Gender_dest', 'Gender') \
                    .withColumnRenamed('Age_dest', 'Age') \
                    .withColumnRenamed('Tenure_dest', 'Tenure') \
                    .withColumnRenamed('Balance_dest', 'Balance') \
                    .withColumnRenamed('NumOfProducts_dest','NumOfProducts') \
                    .withColumnRenamed('HasCrCard_dest','HasCrCard') \
                    .withColumnRenamed('IsActiveMember_dest', 'IsActiveMember') \
                    .withColumnRenamed('EstimatedSalary_dest', 'EstimatedSalary') \
                    .withColumnRenamed('Exited_dest', 'Exited')

display(df_client_final)

# COMMAND ----------

# DBTITLE 1,Load no DataLake camada Silver
df_client_final.coalesce(1).write.mode('overwrite').csv(path='abfss://silver@saneobankaj.dfs.core.windows.net/NeoBank_Modelling', header=True)
