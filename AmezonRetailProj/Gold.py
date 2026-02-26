# Databricks notebook source
class Silver():
    def __init__(self, env):        
        self.catalog = env
        self.silver_db = "silver_db"
        self.gold_db = "gold_db"
        self.sales_summary_daily_tb = "sales_summmary_daily"
        self.sales_summary_catagory = "sales_summary_catagory"
        self.customers_tb = "customers"
        self.sales_tb = "sales"
        self.products_tb = "products"
        import datetime
        
        
    def get_df(self,table_name):
        return spark.read.table(f"{self.catalog}.{self.silver_db}.{table_name}").filter('last_updated = current_date()')


    def upsert_sales_summmary_daily(self):
        from pyspark.sql import functions as F
        sales_df = self.get_df(self.sales_tb)
        
        sales_summmary_daily = sales_df.groupBy("date")
                                .agg(sum(quantity * price).alias("total_sales"))

        sales_summmary_daily.write.mode('append').table(f'{self.catalog}.{self.silver_db}.{self.sales_summary_daily_tb}')
 

    def upsert_sales_summary_catagory(self):
        from pyspark.sql import functions as F

        products_df = self.get_df(self.products_tb)
        sales_df = self.get_df(self.sales_tb)
        
        sales_summary_catagory = sales_df.join( products_df, sales_df.product_id == products_df.product_id)
                                .groupBy("category")
                                .agg( sum(quantity * price).alias("total_sales") )

        sales_summary_catagory.write.mode('overwrite').table(f'{self.catalog}.{self.silver_db}.{self.sales_summary_catagory_tb}')
 
        
        
    def upsert(self):
        import time
        start = int(time.time())
        print(f"\nStarting gold layer upsert ...")
        self.upsert_sales_summmary_daily()
        self.upsert_sales_summary_catagory()
        print(f"Completed gold layer upsert {int(time.time()) - start} seconds")
        
        
    def assert_count(self, table_name, expected_count):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.silver_db}.{table_name}").count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} " 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records : Success")        
        
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating gold layer records...")
        self.assert_count(self.sales_summary_daily_tb, 5 if sets == 1 else 10)
        self.assert_count(self.sales_summary_catagory, 5 if sets == 1 else 10)
        print(f"Gold layer validation completed in {int(time.time()) - start} seconds")                
