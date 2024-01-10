from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

def model(dbt, session):
    def addIdColumn(df, id_column_name):
        columns = df.columns
        new_df = df.withColumn(id_column_name, F.monotonically_increasing_id())
        return new_df[[id_column_name] + columns]

    def generate_new_feature(df):
        return df.withColumn(
            "MY_NEW_FEATURE", df["FIXED_ACIDITY"] * df["CITRIC_ACID"])

    #source_df = session.table(full_table_name)
    source_df = dbt.source("FEATURE_STORE_SRC", "WINE_DATA")
    
    df = addIdColumn(source_df, "WINE_ID")
    feature_df = generate_new_feature(df)
    feature_df = feature_df.select(
        [
            'WINE_ID',
            'FIXED_ACIDITY',
            'VOLATILE_ACIDITY',
            'CITRIC_ACID',
            'RESIDUAL_SUGAR',
            'CHLORIDES',
            'FREE_SULFUR_DIOXIDE',
            'TOTAL_SULFUR_DIOXIDE',
            'DENSITY',
            'PH',
            'MY_NEW_FEATURE',
        ]
    )
    return feature_df