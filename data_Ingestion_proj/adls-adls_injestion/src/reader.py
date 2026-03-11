def read_csv_from_adls(spark, file_path):

    try:

        df = spark.read \
            .format("csv") \
            .option("header", True) \
            .option("inferSchema", True) \
            .load(file_path)

        return df

    except Exception as e:

        raise Exception(f"Error reading CSV: {str(e)}")