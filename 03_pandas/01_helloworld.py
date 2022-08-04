from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F
import numpy as np
import pandas as pd
import pyspark.pandas as ps

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("pandas-test").\
        master("local[*]").\
        getOrCreate()
        
    sc = spark.sparkContext
    
    pandas_series = pd.Series([1,3,5, np.nan, 6, 8])
    
    pyspark_series = ps.Series(pandas_series)
    
    print(pyspark_series)
    
    # Create a pandas DataFrame
    pdf = pd.DataFrame({'A': np.random.rand(5),
                        'B': np.random.rand(5)})
    
    psdf = ps.from_pandas(pdf)
    
    print(psdf.head())