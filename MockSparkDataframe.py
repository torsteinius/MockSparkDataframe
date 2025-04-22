import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Union

"""
Made to make debugging ETL PySpark Dataframes easier, as some frameworks like Synapse Analytics have restrictive debug environments. 
"""

class MockSparkDF:
    def __init__(self, data: Union[pd.DataFrame, List[Dict[str, Any]], Dict[str, List[Any]], List[tuple]]):
        # Accept pandas DataFrame or raw Python data structures
        if isinstance(data, pd.DataFrame):
            self.pdf = data.copy()
        else:
            try:
                self.pdf = pd.DataFrame(data)
            except Exception as e:
                raise ValueError(f"Unsupported data type for MockSparkDF: {e}")
        self.ops = []

    @classmethod
    def from_records(cls, records: List[Dict[str, Any]]):
        """Alternate constructor from list of dict records."""
        return cls(records)

    def filter(self, func):
        self.ops.append(lambda df: df[func(df)])
        return self

    def withColumn(self, colname: str, func):
        self.ops.append(lambda df: df.assign(**{colname: func(df)}))
        return self

    def select(self, *cols: str):
        self.ops.append(lambda df: df[list(cols)])
        return self

    def join(
        self,
        other: 'MockSparkDF',
        on: Union[str, List[str], None] = None,
        how: str = 'inner',
    ) -> 'MockSparkDF':
        """Mimic Spark join by merging underlying pandas DataFrames."""
        def op(df):
            other_df = other.pdf.copy()
            return df.merge(other_df, on=on, how=how)
        self.ops.append(op)
        return self

    def merge(
        self,
        other: 'MockSparkDF',
        on: Union[str, List[str], None] = None,
        how: str = 'inner',
    ) -> 'MockSparkDF':
        """Alias to join(), for compatibility."""
        return self.join(other, on=on, how=how)

    def collect(self) -> List[Dict[str, Any]]:
        df = self.pdf.copy()
        for op in self.ops:
            df = op(df)
        # Return list of row-dicts like Spark's collect()
        return df.to_dict("records")


class MockSparkSession:
    """Mimics SparkSession for local testing."""
    def __init__(self):
        pass

    def createDataFrame(self, data: Union[pd.DataFrame, List[Dict[str, Any]], Dict[str, List[Any]], List[tuple]]) -> MockSparkDF:
        return MockSparkDF(data)


if __name__ == "__main__":
    spark = MockSparkSession()

    # Create two mock dataframes
    df1 = spark.createDataFrame([
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
    ])
    df2 = spark.createDataFrame([
        {"id": 1, "extra": "A"},
        {"id": 2, "extra": "B"},
    ])

    # Perform join and merge
    result_join = (
        df1.join(df2, on="id", how="inner")
           .select("id", "value", "extra")
           .collect()
    )
    print("Join result:", result_join)

    # Merge is alias to join
    result_merge = (
        spark.createDataFrame([{"id": 3, "value": 30}])
             .merge(
                 spark.createDataFrame([{"id": 3, "extra": "C"}]),
                 on="id"
             )
             .collect()
    )
    print("Merge result:", result_merge)
