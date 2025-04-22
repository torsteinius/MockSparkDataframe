import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any


class MockSparkDF:
    def __init__(self, data: Any):
        # Accept pandas DataFrame, list of dicts/tuples, or dict of lists
        if isinstance(data, pd.DataFrame):
            self.pdf = data.copy()
        else:
            # Try to build a pandas DataFrame from raw data
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

    def collect(self):
        df = self.pdf.copy()
        for op in self.ops:
            df = op(df)
        # Return list of row-dicts like Spark's collect()
        return df.to_dict("records")


class MockSparkSession:
    """Mimics SparkSession for local testing."""
    def __init__(self):
        pass

    def createDataFrame(self, data: Any) -> MockSparkDF:
        return MockSparkDF(data)


if __name__ == "__main__":
    # Example usage without needing pandas upfront
    spark = MockSparkSession()
    raw_data = [
        {"id": 1, "name": "Alice", "age": 25, "created_at": datetime.now()},
        {"id": 2, "name": "Bob",   "age": 30, "created_at": datetime.now() - timedelta(days=1)},
        {"id": 3, "name": "Charlie","age": 35, "created_at": datetime.now() - timedelta(days=2)},
    ]

    df = (
        spark.createDataFrame(raw_data)
             .filter(lambda df: df["age"] > 28)
             .withColumn("age_plus_one", lambda df: df["age"] + 1)
             .select("id", "name", "age_plus_one")
    )

    result = df.collect()
    print(result)  # [{'id': 2, 'name': 'Bob', 'age_plus_one': 31}, {'id': 3, 'name': 'Charlie', 'age_plus_one': 36}]
