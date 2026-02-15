from airflow.models import BaseOperator
import pandas as pd
from typing import Optional, Union, List
from pathlib import Path

class FileSaverOperator(BaseOperator):
    """Save DataFrame to CSV or Parquet format

    :param data: DataFrame or list to save
    :param output_path: Output file path
    :param file_format: 'csv' or 'parquet' (default: 'csv')
    :param compression: Parquet compression (default: None)
    :param index: Include index (default: False)
    :param encoding: CSV encoding (default: 'utf-8')
    """

    template_fields = ('output_path',)

    def __init__(self, data: Union[pd.DataFrame, List[pd.DataFrame]], output_path: str,
                 file_format: str = 'csv', compression: Optional[str] = None,
                 index: bool = False, encoding: str = 'utf-8', *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.data, self.output_path, self.file_format = data, output_path, file_format.lower()
        self.compression, self.index, self.encoding = compression, index, encoding

    def execute(self):
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)

        df = pd.concat(self.data, ignore_index=True) if isinstance(self.data, list) else self.data
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise ValueError("Invalid or empty DataFrame")

        (df.to_csv(self.output_path, index=self.index, encoding=self.encoding) if self.file_format == 'csv' else
         df.to_parquet(self.output_path, compression=self.compression, index=self.index) if self.file_format == 'parquet' else
         (_ for _ in ()).throw(ValueError(f"Unsupported format: {self.file_format}")))

        self.log.info(f"Saved to {self.output_path}") 