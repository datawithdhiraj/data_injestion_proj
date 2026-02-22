def write_csv_to_adls(df, output_path):

    try:

        df.coalesce(1).write \
            .format("csv") \
            .option("header", True) \
            .mode("overwrite") \
            .save(output_path)

        print("File written successfully")

    except Exception as e:

        raise Exception(f"Write failed: {str(e)}")