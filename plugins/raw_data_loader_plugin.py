from airflow.models import BaseOperator

import duckdb
import pandas as pd
import os
import re
import csv
from io import StringIO


class RawDataLoaderOperator(BaseOperator):
    """
    Operator to load raw data files into DataFrames.
    
    Enhanced to handle different station .DAT file formats with automatic
    column validation and robust parsing methods.
    
    :param file_path: Path to the raw data file
    :param station: Station name (optional, for validation)
    :param file_type: Type of file (MD, SD, WD) (optional, for validation)
    :param header_sensor: Dictionary containing header information (optional, for validation)
    :param validate_columns: Whether to validate columns against expected headers
    """
    
    template_fields = ('file_path', )
    
    
    def __init__(
        self,
        file_path: str,
        station: str = None,
        file_type: str = None,
        header_sensor: dict = None,
        validate_columns: bool = True,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.station = station
        self.file_type = file_type
        self.header_sensor = header_sensor
        self.validate_columns = validate_columns
        

    def execute(self):
        try:
            # Check if the file exists
            if not os.path.exists(self.file_path):
                self.log.error(f"Error 0 - O arquivo {self.file_path} não existe.")
                return pd.DataFrame()
            
            # Extract station and file type from filename if not provided
            if not self.station or not self.file_type:
                self._extract_metadata_from_filename()
            
            # Try optimized CSV parsing first (fastest for most cases)
            try:
                data = self._read_dat_file_optimized(self.file_path)
                if data is not None and not data.empty:
                    self.log.info(f"Successfully read file using optimized parsing. Shape: {data.shape}")
                    
                    # Validate columns if requested and header_sensor is available
                    if self.validate_columns and self.header_sensor and self.station and self.file_type:
                        data = self._validate_and_fix_columns(data)
                    
                    return data
            except Exception as e:
                self.log.warning(f"Optimized parsing failed: {str(e)}")
            
            # Fallback to manual parsing for complex cases
            try:
                data = self._read_dat_file_manual(self.file_path)
                if data is not None and not data.empty:
                    self.log.info(f"Successfully read file using manual parsing. Shape: {data.shape}")
                    
                    # Validate columns if requested and header_sensor is available
                    if self.validate_columns and self.header_sensor and self.station and self.file_type:
                        data = self._validate_and_fix_columns(data)
                    
                    return data
            except Exception as e:
                self.log.warning(f"Manual parsing failed: {str(e)}")
            
            # Final fallback to duckdb
            try:
                data = duckdb.query(f"""
                    SELECT * 
                    FROM read_csv_auto('{self.file_path}',
                    ignore_errors=true)            
                """).df()
                self.log.info(f"Successfully read file using duckdb. Shape: {data.shape}")
                
                # Validate columns if requested
                if self.validate_columns and self.header_sensor and self.station and self.file_type:
                    data = self._validate_and_fix_columns(data)
                
                return data
            except Exception as e:
                self.log.error(f"Error 1 - Não foi possível ler o arquivo {self.file_path} with any method")
                return pd.DataFrame()
                
        except Exception as e:
            self.log.error(f"Unexpected error: {str(e)}")
            raise
    
    def _extract_metadata_from_filename(self):
        """Extract station and file type from filename"""
        filename = os.path.basename(self.file_path)
        
        # Pattern: STATION_FILETYPE.DAT (e.g., CPA_SD.DAT, PTR_MD.DAT)
        match = re.match(r'^([A-Z]+)_([A-Z]+)\.DAT$', filename)
        if match:
            self.station = match.group(1).lower()
            self.file_type = match.group(2)
            self.log.info(f"Extracted station: {self.station}, file_type: {self.file_type} from filename")
        else:
            self.log.warning(f"Could not extract station and file_type from filename: {filename}")
    
    def _validate_and_fix_columns(self, data):
        """
        Validate and fix column count based on expected headers
        
        Args:
            data: pandas.DataFrame to validate
            
        Returns:
            pandas.DataFrame: Validated and potentially fixed DataFrame
        """
        try:
            if not self.station or not self.file_type or not self.header_sensor:
                return data
            
            # Get expected header for this station and file type
            header_key = f"{self.file_type}_RAW_HEADER"
            if self.station not in self.header_sensor:
                self.log.warning(f"Station {self.station} not found in header_sensor")
                return data
            
            if header_key not in self.header_sensor[self.station]:
                self.log.warning(f"Header key {header_key} not found for station {self.station}")
                return data
            
            expected_headers = self.header_sensor[self.station][header_key]
            expected_count = len(expected_headers)
            actual_count = len(data.columns)
            
            self.log.info(f"Expected columns: {expected_count}, Actual columns: {actual_count}")
            
            # If column counts match, assign headers
            if actual_count == expected_count:
                data.columns = expected_headers
                self.log.info(f"Successfully assigned headers for {self.station}_{self.file_type}")
                return data
            
            # If column counts don't match, try to fix
            elif actual_count > expected_count:
                # Too many columns - truncate to expected count
                self.log.warning(f"Too many columns ({actual_count} > {expected_count}). Truncating.")
                data = data.iloc[:, :expected_count]
                data.columns = expected_headers
                return data
            
            elif actual_count < expected_count:
                # Too few columns - pad with None
                self.log.warning(f"Too few columns ({actual_count} < {expected_count}). Padding with None.")
                missing_cols = expected_count - actual_count
                for i in range(missing_cols):
                    data[f'col_{actual_count + i}'] = None
                data.columns = expected_headers
                return data
            
        except Exception as e:
            self.log.error(f"Error validating columns: {str(e)}")
            return data
        
        return data
    
    def _read_dat_file_optimized(self, file_path):
        """
        Optimized method to read .DAT files using Python's csv module
        
        Args:
            file_path (str): Path to the .DAT file
            
        Returns:
            pandas.DataFrame: Parsed data or None if error
        """
        try:
            # Read the entire file content
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Use StringIO and csv.reader for efficient parsing
            csv_file = StringIO(content)
            csv_reader = csv.reader(csv_file, quotechar='"', delimiter=',')
            
            # Convert to list of lists
            data = list(csv_reader)
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            return df
            
        except Exception as e:
            self.log.error(f"Error reading file with optimized method: {e}")
            return None
    
    def _read_dat_file_manual(self, file_path):
        """
        Read .DAT files using manual parsing for maximum control (fallback method)
        
        Args:
            file_path (str): Path to the .DAT file
            
        Returns:
            pandas.DataFrame: Parsed data or None if error
        """
        try:
            data = []
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        # Split by comma, but respect quoted strings
                        row = []
                        current_field = ""
                        in_quotes = False
                        
                        for char in line:
                            if char == '"':
                                in_quotes = not in_quotes
                            elif char == ',' and not in_quotes:
                                row.append(current_field.strip())
                                current_field = ""
                            else:
                                current_field += char
                        
                        # Add the last field
                        row.append(current_field.strip())
                        data.append(row)
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            self.log.error(f"Error reading file manually: {e}")
            return None