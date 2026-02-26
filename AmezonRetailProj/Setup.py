# Databricks notebook source
class SetupHelper():   
    def __init__(self, env): 
        self.catalog = env
        self.bronze_db = "bronze_db"
        self.silver_db = "silver_db"
        self.gold_db = "gold_db"
        

    def create_db(self):
        print(f"Creating the databases in {self.catalog} env", end='')
        self.create_bronze_layer()
        self.create_silver_layer()
        self.create_gold_layer()
        print("Done")

    def create_bronze_layer(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.bronze_db}")
        self.create_customers(self.bronze_db)
        self.create_Products(self.bronze_db)
        self.create_Sales(self.bronze_db)

    def create_silver_layer(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.silver_db}")
        self.create_customers(self.silver_db)
        self.create_Products(self.silver_db)
        self.create_Sales(self.silver_db)

    def create_gold_layer(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.gold_db}")
        self.create_sales_summary_daily(self.gold_db)
        self.create_sales_summary_catagory(self.gold_db)

    def create_sales_summary_daily(self, db_name):
        print(f"Creating daily sales summary table...", end='')
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{db_name}.sales_summmary_daily(
            date date, 
            total_sales double                  
                )
                """)
        print("Done")

    def create_sales_summary_catagory(self, db_name):
        print(f"Creating category wise sales summary table...", end='')
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{db_name}.sales_summary_catagory(
            category string,
            total_sales double                 
                )
                """) 
        print("Done")

    def create_customers(self, db_name):
        print(f"Creating customers table...", end='')
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{db_name}.customers(
            customer_id long,
            name string,
            email string,
            city string,
            state string,
            signup_date date, 
            last_updated date                  
                )
                """) 
        print("Done")

    def create_Products(self,db_name):
        print(f"Creating products table...", end='')
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{db_name}.products(
            product_id long,
            product_name string,
            category string,
            price double,
            last_updated date              
                )
                """) 
        print("Done")
    
    def create_Sales(self,db_name):
        print(f"Creating sales table...", end='')
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{db_name}.sales(
            transaction_id long,
            customer_id long,
            product_id long,
            quantity long,
            sale_amount double,
            sale_date date,
            last_updated date
                )
                """) 
        print("Done")
            
            
    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_db()       
        print(f"Setup completed in {int(time.time()) - start} seconds")
        
    def assert_table(self, db_name, table_name):
        assert spark.sql(f"SHOW TABLES IN {self.catalog}.{db_name}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.catalog}.{db_name}: Success")
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName in ('{self.bronze_db}','{self.silver_db}','{self.gold_db}')") \
                    .count() == 3, f"The database is missing"
        print(f"Found databases : Success")
        self.assert_table(self.bronze_db,"customers")   
        self.assert_table(self.bronze_db,"products")        
        self.assert_table(self.bronze_db,"sales")
        self.assert_table(self.silver_db,"customers")
        self.assert_table(self.silver_db,"products")
        self.assert_table(self.silver_db,"sales")
        self.assert_table(self.gold_db,"sales_summmary_daily")
        self.assert_table(self.gold_db,"sales_summary_catagory")
        print(f"Setup validation completed in {int(time.time()) - start} seconds")
        
    def cleanup(self): 
        if spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName in ({self.bronze_db,self.silver_db,self.gold_db})") \
                    .count() == 3:
            print(f"Dropping the database in catalog {self.catalog} ...", end='')
            spark.sql(f"DROP DATABASE {self.catalog}.{self.bronze_db} CASCADE")
            spark.sql(f"DROP DATABASE {self.catalog}.{self.silver_db} CASCADE")
            spark.sql(f"DROP DATABASE {self.catalog}.{self.gold_db} CASCADE")
            print("Done")
        print("Done")    
