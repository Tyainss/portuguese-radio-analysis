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

    def _read_schema(self, df, schema):
        # Convert DataFrame columns to the specified data types
        for column, dtype in schema.items():
            logger.info(f'Column : {column}, dtype : {dtype}')
            
            if dtype == pl.Date:
                df = df.with_columns(
                    pl.col(column).str.strptime(pl.Date, format="%Y-%m-%d").cast(dtype, strict=False)
                )
            elif dtype == pl.Time:
                # Detect if time includes nanoseconds
                sample_time = df[column].head(1)[0]
                if '.' in sample_time and len(sample_time.split('.')[1]) > 0:
                    df = df.with_columns(
                        pl.col(column).str.strptime(pl.Time, format="%H:%M:%S%.9f").cast(dtype, strict=False)
                    )
                else:
                    df = df.with_columns(
                        pl.col(column).str.strptime(pl.Time, format="%H:%M").cast(dtype, strict=False)
                    )
            else:
                df = df.with_columns(pl.col(column).cast(dtype, strict=False))

        return df

    def _output_schema(self, df, schema):
        # Convert DataFrame columns to the specified data types
        for column, dtype in schema.items():
            logger.info(f'Processing column: {column}, dtype: {dtype}')

            if dtype == pl.Date:
                df = df.with_columns(
                        pl.col(column).cast(pl.Utf8)
                          .str.strptime(pl.Date, format='%Y-%m-%d', strict=False)
                          .alias(column)
                )
            elif dtype == pl.Time:
                df = df.with_columns(
                        pl.col(column).cast(pl.Utf8)
                          .str.strptime(pl.Time, format="%H:%M", strict=False)
                          .alias(column)
                )
            elif dtype == pl.Utf8:
                # Fill nulls with empty string to match String dtype
                df = df.with_columns(
                    pl.col(column).fill_null("").cast(dtype, strict=False)
                )
            else:
                df = df.with_columns(pl.col(column).cast(dtype, strict=False))

        return df

    def read_excel(self, path: str, schema: Optional[Dict[str, pl.DataType]] = None) -> pl.DataFrame:
        logger.info(f'Reading Excel from: {path}')

        if not os.path.exists(path):
            logger.warning(f'Excel file not found: {path}')
            return pl.DataFrame()
        
        df = pl.read_excel(path)

        if schema:
            df = self._read_schema(df, schema)

        return df

    def read_csv(self, path: str, schema: Optional[Dict[str, pl.DataType]] = None) -> pl.DataFrame:
        logger.info(f'Reading CSV from: {path}')

        if not os.path.exists(path):
            logger.warning(f'CSV file not found: {path}')
            return pl.DataFrame()
        
        df = pl.read_csv(path)

        if schema:
            df = self._read_schema(df, schema)
        
        return df

    def output_excel(self, path: str, df: pl.DataFrame, schema: Optional[Dict[str, pl.DataType]] = None, append: bool = False) -> None:
        logger.info(f'Outputting Excel to: {path}')
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Ensure dataframe is not empty:
        if df.is_empty():
            logger.info('Dataframe is empty. Skipping outputting')
            return
        
        if schema:
            df = self._output_schema(df, schema)
        
        if os.path.exists(path) and append:
            try:
                existing_df = self.read_excel(path=path, schema=schema)
                df = pl.concat([existing_df, df])
            except Exception as e:
                logger.error(f"Error reading existing Excel for appending: {e}")
                # Continue with writing the new data even if appending fails
        
        try:
            df.write_excel(path)
            logger.info('Successfully updated Excel')
        except Exception as e:
            logger.error(f"Error writing Excel: {e}")
            raise

    def output_csv(self, path: str, df: pl.DataFrame, schema: Optional[Dict[str, pl.DataType]] = None, append: bool = False) -> None:
        logger.info(f'Outputting CSV to: {path}')
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Ensure dataframe is not empty:
        if df.is_empty():
            logger.info('Dataframe is empty. Skipping outputting')
            return
        
        if schema:
            df = self._output_schema(df, schema)
        
        if os.path.exists(path) and append:
            try:
                existing_df = self.read_csv(path=path, schema=schema)
                df = pl.concat([existing_df, df])
            except Exception as e:
                logger.error(f"Error reading existing CSV for appending: {e}")
                # Continue with writing the new data even if appending fails
        
        try:
            df.write_csv(path)
            logger.info('Successfully updated CSV')
        except Exception as e:
            logger.error(f"Error writing CSV: {e}")
            raise