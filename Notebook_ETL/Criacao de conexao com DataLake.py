# Databricks notebook source
# MAGIC %md
# MAGIC Criando conexão com DataLake
# MAGIC

# COMMAND ----------

client_id = '51309d2a-6365-4227-a514-ba686a108992'
tenant_id = 'd8bf25c8-7b76-4f73-8b71-0a00aebcd545'
client_secret = 'mMD8Q~BWJnciDYTiz4qvWzDedFQka5E31oTqdcbN'
storage_account = 'saneobankaj'
conteiner_name = 'bronze'

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": f"{client_id}",
"fs.azure.account.oauth2.client.secret":  f"{client_secret}",
"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC Montando Diretorio

# COMMAND ----------

dbutils.fs.mount(
source = f"abfss://{conteiner_name}@{storage_account}.dfs.core.windows.net/",
mount_point = f"/mnt/{storage_account}/{conteiner_name}",
extra_configs = configs
)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

df_NeoBank = spark.read.csv('dbfs:/mnt/saneobankaj/bronze/NeoBank_Modelling.csv', header = True)

display(df_NeoBank)
