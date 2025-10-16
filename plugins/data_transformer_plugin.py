from airflow.models import BaseOperator

import pandas as pd

from typing import  Dict, Union, List
import traceback


class DataTransformerOperator(BaseOperator):
    """
    Operator to transform raw data into processed format.
    
    Generalized operator that can handle any station and file type by
    intelligently determining the correct headers to apply.
    
    :param data: DataFrame or list of DataFrames to transform
    :param station: Station name
    :param file_type: Type of file (MD, SD, WD)
    :param header_sensor: Dictionary containing sensor header information
    """
    template_fields = ('file_type',)
        
    def __init__(
        self,
        data: Union[pd.DataFrame, List[pd.DataFrame]],
        station: str,
        file_type: str,
        header_sensor: Dict,        
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.data = data
        self.station = station
        self.file_type = file_type        
        self.header_sensor = header_sensor

    def execute(self):
        try:
            # Convert list of DataFrames to single DataFrame if necessary
            if isinstance(self.data, list):
                if not self.data:
                    raise ValueError("Empty list of DataFrames provided")
                df = pd.concat(self.data, ignore_index=True)
            else:
                df = self.data
            
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Expected DataFrame or list of DataFrames, got {type(df)}")
            
            # Check if station exists in header_sensor
            station_key = self.station
            if station_key not in self.header_sensor:
                raise KeyError(f"Station {station_key} not found in header_sensor")
            
            # Get the raw header key
            header_key = f"{self.file_type}_RAW_HEADER"
            if header_key not in self.header_sensor[self.station]:
                raise KeyError(f"Header key {header_key} not found for station {self.station}")
            
            raw_headers = self.header_sensor[self.station][header_key]
            self.log.info(f"Raw headers for {self.station}_{self.file_type}: {len(raw_headers)} columns")
            
            # Determine the correct headers to apply based on data structure
            final_headers = self._determine_final_headers(df, raw_headers)
            
            # Apply the headers
            if len(df.columns) != len(final_headers):
                self.log.warning(f"Column count mismatch: data has {len(df.columns)}, headers has {len(final_headers)}")
                # Try to fix by truncating or padding
                df = self._fix_column_count(df, final_headers)
            
            df.columns = final_headers
            self.log.info(f"Successfully applied {len(final_headers)} headers to {self.station}_{self.file_type}")
            
            return df

        except Exception as e:
            self.log.error(f"Error transforming data for {self.station}_{self.file_type}: {str(e)}")
            return None
    
    def _determine_final_headers(self, df: pd.DataFrame, raw_headers: List[str]) -> List[str]:
        """
        Intelligently determine which headers to apply based on data structure.
        
        Args:
            df: DataFrame to transform
            raw_headers: Complete raw headers from configuration
            
        Returns:
            List[str]: Headers to apply to the DataFrame
        """
        actual_columns = len(df.columns)
        total_headers = len(raw_headers)
        
        self.log.info(f"Data has {actual_columns} columns, raw headers has {total_headers} columns")
        
        # If the data already has the same number of columns as raw headers,
        # it means headers were already applied by RawDataLoaderOperator
        if actual_columns == total_headers:
            self.log.info("Headers already applied by RawDataLoaderOperator, returning data as-is")
            return list(df.columns)
        
                    # If data has fewer columns than raw headers, we need to determine
            # which subset of headers to apply
            if actual_columns < total_headers:
                # Try different patterns to find the right subset
                patterns = [
                    (8, -2),  # TOA5 pattern: skip first 8 metadata columns and last 2
                    (0, -2),  # Skip last 2
                    (2, None),  # Skip first 2
                    (1, -1),  # Skip first and last
                    (0, None),  # Use all headers
                ]
                
                for start, end in patterns:
                    if start is None:
                        subset_headers = raw_headers[:end]
                    elif end is None:
                        subset_headers = raw_headers[start:]
                    else:
                        subset_headers = raw_headers[start:end]
                    
                    if len(subset_headers) == actual_columns:
                        pattern_name = "TOA5" if (start, end) == (8, -2) else f"[{start}:{end}]"
                        self.log.info(f"Applied {pattern_name} pattern: {len(subset_headers)} columns")
                        return subset_headers
            
            # If no pattern works, truncate headers to match data
            self.log.warning(f"No header pattern found, truncating headers to {actual_columns} columns")
            return raw_headers[:actual_columns]
        
        # If data has more columns than raw headers, pad headers
        else:
            self.log.warning(f"Data has more columns than headers, padding headers")
            padded_headers = raw_headers.copy()
            for i in range(actual_columns - total_headers):
                padded_headers.append(f"extra_col_{i+1}")
            return padded_headers
    
    def _fix_column_count(self, df: pd.DataFrame, target_headers: List[str]) -> pd.DataFrame:
        """
        Fix column count mismatch by truncating or padding the DataFrame.
        
        Args:
            df: DataFrame to fix
            target_headers: Target headers to match
            
        Returns:
            pd.DataFrame: Fixed DataFrame
        """
        actual_columns = len(df.columns)
        target_columns = len(target_headers)
        
        if actual_columns > target_columns:
            # Truncate DataFrame
            self.log.info(f"Truncating DataFrame from {actual_columns} to {target_columns} columns")
            return df.iloc[:, :target_columns]
        else:
            # Pad DataFrame with None values
            self.log.info(f"Padding DataFrame from {actual_columns} to {target_columns} columns")
            missing_cols = target_columns - actual_columns
            for i in range(missing_cols):
                df[f'pad_col_{i+1}'] = None
            return df