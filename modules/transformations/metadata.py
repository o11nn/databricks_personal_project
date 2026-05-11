from pysparl.sql import DataFrame
from pyspark.sql.functions import current_timestamp

def add_processed_timestamp(df: DataFrame) -> DataFrame:
    
    """
    Adds a 'processed_timestamp' column to DataFrame with the current_timestamp
    
    df - the input Spark DataFrame
    
    returns - The DataFrame with the additional 'processed_timestamp' column
    """
    return df.withColumn("processed_timestamp", current_timestamp())