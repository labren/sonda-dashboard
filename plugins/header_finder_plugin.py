from airflow.models import BaseOperator
from airflow.providers.ftp.hooks.ftp import FTPHook

import pandas as pd
import os
import re
import csv
from io import StringIO
from collections import deque
import json


class HeaderFinderOperator(BaseOperator):
    """
    Operator to find and extract headers from raw .DAT files on FTP.
    
    Downloads the first 20 lines of each file, parses them to identify headers,
    and returns the extracted header information.
    
    :param station: Station name
    :param file_type: Type of file (MD, SD, WD, TD)
    :param remote_file_path: Remote file path on FTP
    :param ftp_conn_id: FTP connection ID
    :param lines_to_download: Number of lines to download for header detection
    """
    
    template_fields = ('remote_file_path', )
    
    def __init__(
        self,
        station: str,
        file_type: str,
        remote_file_path: str,
        ftp_conn_id: str = 'solter.ftp.1',
        lines_to_download: int = 20,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.station = station
        self.file_type = file_type
        self.remote_file_path = remote_file_path
        self.ftp_conn_id = ftp_conn_id
        self.lines_to_download = lines_to_download
    
    def execute(self):
        """Execute the header finding operation"""
        try:
            self.log.info(f"Finding headers for {self.station}_{self.file_type} from {self.remote_file_path}")
            
            # Download first N lines from FTP
            lines = self._download_file_lines()
            if not lines:
                self.log.error(f"Failed to download lines from {self.remote_file_path}")
                return None
            
            # Parse headers from the lines
            headers = self._extract_headers(lines)
            if not headers:
                self.log.warning(f"No headers found for {self.station}_{self.file_type}")
                return None
            
            self.log.info(f"Found {len(headers)} headers for {self.station}_{self.file_type}")
            
            return {
                'station': self.station,
                'file_type': self.file_type,
                'headers': headers,
                'header_key': f"{self.file_type}_RAW_HEADER"
            }
            
        except Exception as e:
            self.log.error(f"Error finding headers for {self.station}_{self.file_type}: {str(e)}")
            return None
    
    def _download_file_lines(self):
        """Download first N lines from FTP file"""
        try:
            ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)
            ftp_conn = ftp_hook.get_conn()
            
            # Use a deque to keep only the first N lines
            lines = deque(maxlen=self.lines_to_download)
            
            def handle_binary(more_data):
                data_lines = more_data.decode('utf-8', errors='ignore').splitlines(keepends=True)
                for line in data_lines:
                    if len(lines) < self.lines_to_download:
                        lines.append(line)
                    else:
                        break  # Stop after getting enough lines
            
            # Retrieve the file from FTP, processing it in chunks
            ftp_conn.retrbinary(f"RETR {self.remote_file_path}", callback=handle_binary)
            
            return list(lines)
            
        except Exception as e:
            self.log.error(f"Error downloading file lines: {str(e)}")
            return []
    
    def _extract_headers(self, lines):
        """
        Extract headers from the downloaded lines
        
        Args:
            lines: List of lines from the file
            
        Returns:
            list: Extracted headers or None if not found
        """
        try:
            # Convert lines to string for processing
            content = ''.join(lines)
            
            # Try different parsing methods
            headers = self._parse_headers_csv(content)
            if headers:
                return headers
            
            headers = self._parse_headers_manual(content)
            if headers:
                return headers
            
            return None
            
        except Exception as e:
            self.log.error(f"Error extracting headers: {str(e)}")
            return None
    
    def _parse_headers_csv(self, content):
        """Parse headers using CSV reader"""
        try:
            # Use StringIO and csv.reader for parsing
            csv_file = StringIO(content)
            csv_reader = csv.reader(csv_file, quotechar='"', delimiter=',')
            
            # Get all rows
            rows = list(csv_reader)
            
            if len(rows) < 2:
                return None
            
            # Look for header row (usually row 8 in TOA5 format)
            # TOA5 format typically has metadata in first 7 rows, headers in row 8
            for i, row in enumerate(rows[:10]):  # Check first 10 rows
                if len(row) > 5 and any('TIMESTAMP' in str(cell) for cell in row):
                    # Found header row
                    headers = [str(cell).strip() for cell in row]
                    self.log.info(f"Found headers in row {i+1}: {len(headers)} columns")
                    return headers
            
            return None
            
        except Exception as e:
            self.log.debug(f"CSV parsing failed: {str(e)}")
            return None
    
    def _parse_headers_manual(self, content):
        """Parse headers using manual parsing for complex cases"""
        try:
            lines = content.splitlines()
            
            # Look for line with TIMESTAMP (typical header indicator)
            for i, line in enumerate(lines[:10]):
                if 'TIMESTAMP' in line:
                    # Parse this line manually
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
                    
                    if len(row) > 5:  # Must have reasonable number of columns
                        self.log.info(f"Found headers in line {i+1}: {len(row)} columns")
                        return row
            
            return None
            
        except Exception as e:
            self.log.debug(f"Manual parsing failed: {str(e)}")
            return None


class HeaderConfigUpdaterOperator(BaseOperator):
    """
    Operator to update the cabecalho_sensor.json file with new headers.
    
    :param new_headers: List of header information dictionaries
    :param config_file_path: Path to the cabecalho_sensor.json file
    """
    
    def __init__(
        self,
        new_headers: list,
        config_file_path: str = '/opt/airflow/config_files/cabecalho_sensor.json',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.new_headers = new_headers
        self.config_file_path = config_file_path
    
    def execute(self):
        """Update the configuration file with new headers"""
        try:
            self.log.info(f"Updating header configuration with {len(self.new_headers)} new headers")
            
            # Load existing configuration
            if os.path.exists(self.config_file_path):
                with open(self.config_file_path, 'r') as f:
                    config = json.load(f)
            else:
                config = {}
            
            # Update with new headers
            updated_count = 0
            for header_info in self.new_headers:
                if header_info is None:
                    continue
                
                station = header_info['station']
                header_key = header_info['header_key']
                headers = header_info['headers']
                
                # Initialize station if it doesn't exist
                if station not in config:
                    config[station] = {}
                
                # Add or update header
                config[station][header_key] = headers
                updated_count += 1
                
                self.log.info(f"Updated headers for {station}_{header_key}: {len(headers)} columns")
            
            # Save updated configuration
            with open(self.config_file_path, 'w') as f:
                json.dump(config, f, indent=4)
            
            self.log.info(f"Successfully updated {updated_count} header configurations")
            return f"Updated {updated_count} header configurations"
            
        except Exception as e:
            self.log.error(f"Error updating header configuration: {str(e)}")
            raise 