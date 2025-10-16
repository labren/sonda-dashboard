from airflow.models import BaseOperator
import pandas as pd
from typing import Optional, Union, List
import os
from pathlib import Path

class FileSaverOperator(BaseOperator):
    """
    Operator to save data to CSV or Parquet format with error handling.
    
    :param data: DataFrame or list of DataFrames to save
    :param output_path: Path where the file should be saved
    :param file_format: Format to save the file ('csv' or 'parquet')
    :param compression: Compression type for parquet files (default: None)
    :param index: Whether to include index in CSV (default: False)
    :param encoding: File encoding for CSV (default: 'utf-8')
    """
    
    # List of fields that can be templated with Jinja
    # This allows output_path to be dynamically set using Airflow variables/macros
    template_fields = ('output_path',)
    
    
    def __init__(
        self,
        data: Union[pd.DataFrame, List[pd.DataFrame]],
        output_path: str,
        file_format: str = 'csv',
        compression: Optional[str] = None,
        index: bool = False,
        encoding: str = 'utf-8',
        *args,
        **kwargs
        
    ) -> None:
        super().__init__(*args, **kwargs)
        self.data = data
        self.output_path = output_path
        self.file_format = file_format.lower()
        self.compression = compression
        self.index = index
        self.encoding = encoding

    def execute(self):
        try:
            # Ensure output directory exists
            output_dir = os.path.dirname(self.output_path)
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Convert list of DataFrames to single DataFrame if necessary
            if isinstance(self.data, list):
                if not self.data:
                    raise ValueError("Empty list of DataFrames provided")
                df = pd.concat(self.data, ignore_index=True)
            else:
                df = self.data
            
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Expected DataFrame or list of DataFrames, got {type(df)}")
            
            # Check if DataFrame is empty
            if df.empty:
                raise ValueError("Cannot save empty DataFrame")
            
            # Save based on format
            if self.file_format == 'csv':
                df.to_csv(
                    self.output_path,
                    index=self.index,
                    encoding=self.encoding
                )
            elif self.file_format == 'parquet':
                df.to_parquet(
                    self.output_path,
                    compression=self.compression,
                    index=self.index
                )
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}. Use 'csv' or 'parquet'")
            
            self.log.info(f"Successfully saved data to {self.output_path}")
            
        except Exception as e:
            self.log.error(f"Error saving file: {str(e)}")
            raise 