# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %run ./Config
# MAGIC

# COMMAND ----------


import datetime
from pyspark.sql.functions import lit

# COMMAND ----------

class Bronze():
    def __init__(self, env):        
        self.Conf = Config()
        self.landing_zone = self.Conf.base_dir_data + "/AmezonRetailRaw/" 
        self.catalog = env
        self.bronze_db = "bronze_db"
        self.customers_file_ini = "Customers_"
        self.sales_file_ini = "Sales_"
        self.product_file_ini = "Products_"

    def get_file_Name_path(self,file_ini):
        date = datetime.date.today().strftime("%Y-%m-%d")
        dateFolder = datetime.date.today().strftime("/%Y/%m/%d/")
        file_name =  file_ini + date + ".txt"
        file_path = self.landing_zone + dateFolder  + file_name
        return (file_name,file_path)

    def consume_customers(self):
        (file_name,file_path) = self.get_file_Name_path(self.customers_file_ini)
        file_exists = any(f.name == file_name for f in dbutils.fs.ls(self.landing_zone))
        if file_exists:
            schema = '''customer_id long,
                        name string,
                        email string,
                        city string,
                        state string,
                        signup_date date, 
                        last_updated date'''

            customers_df = (spark.read
                            .format("csv")
                            .option("header", "true")
                            .schema(schema)
                            .load(file_path))

            customers_df.write.mode("append").saveAsTable(f"{self.catalog}.{self.bronze_db}.customers")
        else:
            print("No updates today for customers")

        
    def consume_products(self):
        (file_name,file_path) = self.get_file_Name_path(self.product_file_ini)
        file_exists = any(f.name == file_name for f in dbutils.fs.ls(self.landing_zone))
        if file_exists:
            schema = '''product_id long,
                        product_name string,
                        category string,
                        price double,
                        last_updated date'''

            customers_df = (spark.read
                            .format("csv")
                            .option("header", "true")
                            .schema(schema)
                            .load(file_path))

            customers_df.write.mode("append").saveAsTable(f"{self.catalog}.{self.bronze_db}.products")
        else:
            print("No updates today for customers")

    def consume_sales(self):
        (file_name,file_path) = self.get_file_Name_path(self.sales_file_ini )
        file_exists = any(f.name == file_name for f in dbutils.fs.ls(self.landing_zone))
        if file_exists:
            schema = '''transaction_id long,
                        customer_id long,
                        product_id long,
                        quantity long,
                        sale_amount double,
                        sale_date date,
                        last_updated date'''

            customers_df = (spark.read
                            .format("csv")
                            .option("header", "true")
                            .schema(schema)
                            .load(file_path))

            customers_df.write.mode("append").saveAsTable(f"{self.catalog}.{self.bronze_db}.sales")
        else:
            print("No updates today for sales")
            
    def consume(self):
        import time
        start = int(time.time())
        print(f"\nStarting bronze layer consumption ...")
        self.consume_customers() 
        self.consume_products() 
        self.consume_sales() 
        print(f"Completed bronze layer consumtion {int(time.time()) - start} seconds")
        
        
    def assert_count(self, table_name, expected_count):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.bronze_db}.{table_name}").count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} " 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records : Success")        
        
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating bronz layer records...")
        self.assert_count("customers", 5 if sets == 1 else 10)
        self.assert_count("products", 5 if sets == 1 else 10)
        self.assert_count("sales", 5 if sets == 1 else 10)
        print(f"Bronze layer validation completed in {int(time.time()) - start} seconds")                
