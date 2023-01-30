# Databricks notebook source
x=5
y=10
print(x+y)

# COMMAND ----------

storage_account_name = "hakkimdatalake"
client_id = "cfe7d8d0-9a0a-47f1-85b1-3f85f277abc6"
tenant_id = "af044cae-10ec-4f63-bddb-31706181275f"
client_secret = "AVI8Q~Jr6wcUeTh~e2tK4n9Yf5UjW.qMDVVmQaeJ"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id" : f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls('datalakecontainer')

# COMMAND ----------

dbutils.fs.ls ("/mnt/hakkimdatalake/datalakecontainer")

# COMMAND ----------

df= spark.read.csv("dbfs:/mnt/hakkimdatalake/datalakecontainer/countrylookup.csv", header=True)

# COMMAND ----------

display(df)
