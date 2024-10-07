import polars as pl
import os
from typing import Optional, Dict

# Import the logging configuration
from logger import setup_logging

# Set up logging
logger = setup_logging()

class DataStorage:
    def __init__(self):
        pass

    def read_excel(self, path: str, schema: Optional[Dict[str, pl.DataType]] = None) -> pl.DataFrame:
        logger.info(f'Reading Excel from: {path}')
        df = pl.read_excel(path)
        if schema:
            # Convert DataFrame columns to the specified data types
            for column, dtype in schema.items():
                logger.info(f'Column : {column}, dtype : {dtype}')
                df = df.with_columns(pl.col(column).cast(dtype))
        
        return df

    def output_excel(self, path: str, df: pl.DataFrame, schema: Optional[Dict[str, pl.DataType]] = None, append: bool = False) -> None:
        logger.info(f'Outputting Excel to: {path}')
        if schema:
            # Convert DataFrame columns to the specified data types
            for column, dtype in schema.items():
                logger.info(f'Column :{column}, dtype :{dtype}')
                df = df.with_columns(pl.col(column).cast(dtype))
        
        if os.path.exists(path) and append:
            existing_df = self.read_excel(path=path, schema=schema)
            df = pl.concat([existing_df, df])
        
        df.write_excel(path)