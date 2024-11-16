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
            # logger.info(f'Processing column: {column}, dtype: {dtype}')

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


    def read_csv(self, path: str, schema: Optional[Dict[str, pl.DataType]] = None) -> pl.DataFrame:
        logger.info(f'Reading CSV from: {path}')

        if not os.path.exists(path):
            logger.warning(f'CSV file not found: {path}')
            return pl.DataFrame()
        
        df = pl.read_csv(path)

        if schema:
            df = self._read_schema(df, schema)
        
        return df

    def output_csv(
            self, 
            path: str, 
            df: pl.DataFrame, 
            schema: Optional[Dict[str, pl.DataType]] = None, 
            mode: str = 'append'
        ) -> None:
        logger.info(f'Outputting CSV to: {path}')
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Ensure dataframe is not empty:
        if df.is_empty():
            logger.info('Dataframe is empty. Skipping outputting')
            return
        
        if schema:
            df = self._output_schema(df, schema)
        
        if os.path.exists(path):
            try:
                existing_df = self.read_csv(path=path, schema=schema)
                
                if mode == 'append':
                    df = pl.concat([existing_df, df])
                elif mode == 'deduplicate_append':
                    df = pl.concat([existing_df, df]).unique()
                elif mode == 'overwrite':
                    logger.info('Overwritting existing CSV with new data')
                else:
                    logger.error(f'Invalid mode: {mode}')
                    raise ValueError(f"Invalid mode: {mode}. Use 'overwrite', 'append', or 'deduplicate_append'.")
                
            except Exception as e:
                logger.error(f"Error reading existing CSV for appending: {e}")
                if mode != 'overwrite':
                    raise
        
        try:
            df.write_csv(path)
            logger.info(f'Successfully updated CSV in {mode} mode')
        except Exception as e:
            logger.error(f"Error writing CSV: {e}")
            raise

    def read_csv_if_exists(self, path, schema, columns):
        """
        Reads a CSV file if it exists and selects specified columns.

        Parameters:
        data_storage (DataStorage): The data storage instance to read CSV.
        path (str): The path to the CSV file.
        schema (dict): The schema of the CSV file.
        columns (list): The columns to select.

        Returns:
        pl.DataFrame: The resulting DataFrame with selected columns or an empty DataFrame.
        """
        try:
            if os.path.exists(path):
                df = self.read_csv(path=path, schema=schema).select(columns)
            else:
                logger.warning(f"File {path} does not exist. Returning an empty DataFrame.")
                df = pl.DataFrame(schema={col: schema[col] for col in columns if col in schema})

        except pl.exceptions.ColumnNotFoundError as e:
            logger.error(f"Column not found in {path}: {e}")
            df = pl.DataFrame(schema={col: schema[col] for col in columns if col in schema})

        except Exception as e:
            logger.error(f"Error reading {path}: {e}")
            df = pl.DataFrame(schema={col: schema[col] for col in columns if col in schema})

        return df