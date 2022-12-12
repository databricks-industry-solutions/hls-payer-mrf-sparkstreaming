# Databricks notebook source
# MAGIC %md 
# MAGIC # Custom Spark Streaming Source - Payer MRF
# MAGIC 
# MAGIC ## Use Case 
# MAGIC 
# MAGIC 
# MAGIC CMS Schemas for MRF are built using a single json object which spark by default cannot split. Reading these large files often results in Out of Memory errors. This parser serves to transform any array objects under the header into multiple splittable lists that can be parsed. See sample schemas here ->  https://github.com/CMSgov/price-transparency-guide/tree/master/schemas
# MAGIC 
# MAGIC 
# MAGIC ## Where to get MRF Files? 
# MAGIC 
# MAGIC Here are some of the USA's larger payers and landing page
# MAGIC 1. UHG https://transparency-in-coverage.uhc.com/
# MAGIC 2. Anthem https://www.anthem.com/machine-readable-file/search/
# MAGIC 3. Cigna https://www.cigna.com/legal/compliance/machine-readable-files
# MAGIC 4. Aetna https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICSI/machine-readable-transparency-in-coverage?reportingEntityType=Third%20Party%20Administrator_6644&lock=true
# MAGIC 5. Humana https://developers.humana.com/syntheticdata/Resource/PCTFilesList?fileType=innetwork

# COMMAND ----------


