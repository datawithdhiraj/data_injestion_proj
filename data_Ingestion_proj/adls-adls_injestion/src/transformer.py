from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

def transform_employee_data(df):

    df = df.dropDuplicates()

    df = df.fillna({
        "salary": 0
    })

    df = df.withColumn(
        "salary",
        col("salary").cast(DoubleType())
    )

    df = df.filter(col("salary") > 30000)

    return df