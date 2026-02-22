import sys

# Add repo path
sys.path.append("/Workspace/Users/dhiraj.dhande.de@gmail.com/data_injestion_proj/adls-adls_injestion")

from src.transformer import transform_employee_data


def test_salary_filter():

    data = [
        (1, "Amit", 40000),
        (2, "Rahul", 20000)
    ]

    columns = ["id", "name", "salary"]

    # Use existing Databricks spark session
    df = spark.createDataFrame(data, columns)

    result = transform_employee_data(df)

    assert result.count() == 1