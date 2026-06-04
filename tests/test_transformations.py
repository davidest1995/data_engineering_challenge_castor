import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("local-tests") \
        .getOrCreate()

def clean_monto(df):
    """
    Función de prueba para limpiar la columna 'monto'
    Elimina '$', ',' y espacios, luego castea a Double.
    """
    return df.withColumn(
        "monto",
        F.regexp_replace(F.col("monto"), r"[\$,\s]", "").cast("double")
    )

def test_clean_monto(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("monto", StringType(), True)
    ])
    data = [
        ("1", "$ 1,000.50"),
        ("2", "500"),
        ("3", "$2,500.00")
    ]
    df = spark.createDataFrame(data, schema)
    
    result_df = clean_monto(df)
    results = result_df.collect()
    
    assert results[0]["monto"] == 1000.50
    assert results[1]["monto"] == 500.0
    assert results[2]["monto"] == 2500.0
