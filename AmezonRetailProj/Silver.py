# Databricks notebook source
class Silver():
    def __init__(self, env):        
        self.catalog = env
        self.silver_db = "silver_db"
        self.bronze_db = "bronze_db"
        self.customers_tb = "customers"
        self.sales_tb = "sales"
        self.products_tb = "products"
        import datetime
        
        
    def get_df(self,table_name):
        return spark.read.table(f"{self.catalog}.{self.bronze_db}.{table_name}").filter('last_updated = current_date()')


    def upsert_customers(self):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        customers_df = self.get_df(self.customers_tb)
        # Keep only the latest record per customer_id based on last_modified_date
        latest_customers_df = (
            customers_df
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("customer_id").orderBy(F.col("last_updated").desc())
            ))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        delta_table = DeltaTable.forName(spark, f"{self.catalog}.{self.silver_db}.customers")
        (
            delta_table.alias("t")
            .merge(
                latest_customers_df.alias("s"),
                "t.customer_id = s.customer_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def upsert_products(self):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        products_df = self.get_df(self.products_tb)

        # Keep only the latest record per product_id based on last_modified_date
        latest_products_df = (
            products_df
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("product_id").orderBy(F.col("last_updated").desc())
            ))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        delta_table = DeltaTable.forName(spark, f"{self.catalog}.{self.silver_db}.products")
        (
            delta_table.alias("t")
            .merge(
                latest_products_df.alias("s"),
                "t.product_id = s.product_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        
    def upsert_sales(self):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        sales_df = self.get_df(self.sales_tb)
        # Keep only the latest record per product_id based on last_modified_date
        latest_sales_df = (
            sales_df
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("transaction_id").orderBy(F.col("last_updated").desc())
            ))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )
    
        delta_table = DeltaTable.forName(spark, f"{self.catalog}.{self.silver_db}.sales")
        (
            delta_table.alias("t")
            .merge(
                latest_sales_df.alias("s"),
                "t.transaction_id = s.transaction_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
    def upsert(self):
        import time
        start = int(time.time())
        print(f"\nStarting silver layer upsert ...")
        self.upsert_customers()
        self.upsert_products()
        self.upsert_sales()
        print(f"Completed silver layer upsert {int(time.time()) - start} seconds")
        
        
    def assert_count(self, table_name, expected_count):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.silver_db}.{table_name}").count()
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
