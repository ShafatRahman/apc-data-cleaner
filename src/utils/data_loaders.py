import os
import glob
import pandas as pd
from typing import Set
from src.exceptions.data_loading import DataLoaderError

class BaseDataLoader:
    def __init__(self, folder_paths: Set[str], file_patterns: Set[str] = {"*.csv"}):
        if not isinstance(folder_paths, set):
            raise TypeError("folder_paths must be a set of strings")
        try:
            self.folder_paths = folder_paths
            self.file_patterns = file_patterns
            self.data_loaded = False
            self.df = self._load_data()
        except Exception as e:
            raise DataLoaderError(f"Error initializing data loader: {str(e)}")

    def get_data(self) -> pd.DataFrame:
        try:
            if self.df is None:
                raise ValueError("No data has been loaded")
            return self.df
        except Exception as e:
            raise DataLoaderError(f"Error getting data: {str(e)}")

    def add_folder_path(self, folder_path: str) -> None:
        try:
            if not os.path.exists(folder_path):
                raise ValueError(f"Folder path does not exist: {folder_path}")
            self.folder_paths.add(folder_path)
            self.data_loaded = False
            self._append_data(folder_path)
        except Exception as e:
            raise DataLoaderError(f"Error adding folder path: {str(e)}")

    def clear_folder_paths(self):
        self.folder_paths = []
        self.data_loaded = False
        self.df = None

    def remove_folder_path(self, folder_to_remove: str):
        self.folder_paths.remove(folder_to_remove)
        self.data_loaded = False
        self.df = None

    def _load_data(self) -> pd.DataFrame:
        # Subclass must implement this method
        raise NotImplementedError("Not implemented")

    def _append_data(self, folder_path: str):
        # Subclass must implement this method
        raise NotImplementedError("Not implemented")


class APCDataLoader(BaseDataLoader):
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Drop empty rows
            df = df.dropna(how='all')

            # Drop duplicate header rows
            header_mask = df.apply(lambda x: x.astype(str).str.lower()) == df.columns.values
            df = df[~header_mask]
            
            # Convert to ISO 8601 datetime
            df['event_timestamp'] = df['date'] + 'T' + df['time']
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], format='ISO8601')
            df = df.drop(columns=['date', 'time', 'dwell time'], axis=1)
            
            return df
        except Exception as e:
            raise DataLoaderError(f"Error cleaning data: {str(e)}")

    def _enforce_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            required_columns = ['event_timestamp', 'vehicle_id', 'ons', 'offs', 'longitude', 'latitude']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            return df[required_columns]
        except Exception as e:
            raise DataLoaderError(f"Error enforcing required columns: {str(e)}")

    def _load_data(self) -> pd.DataFrame:
        try:
            if not self.data_loaded:
                for folder_path in self.folder_paths:
                    self._append_data(folder_path, self.file_patterns)
                self.data_loaded = True
        except Exception as e:
            raise DataLoaderError(f"Error loading data: {str(e)}")
        
        return self.df

    def _append_data(self, folder_path: str, file_patterns: str):
        try:
            dfs = []
            expected_headers = ['date', 'time', 'ons', 'offs', 'longitude', 'latitude', "dwell time"]
            for pattern in file_patterns:
                files = glob.glob(os.path.join(folder_path, pattern))
                if not files:
                    raise ValueError(f"No files found matching pattern: {pattern}")
                
                for file in files:
                    try:
                        # Read first row to check if it's headers
                        first_row = pd.read_csv(file, nrows=1, header=None)
                        first_row_values = [str(x).lower() for x in first_row.values[0]]
                        has_headers = all([header in first_row_values for header in expected_headers])
                        
                        if has_headers:
                            # If headers exist, read with header row and convert to lowercase
                            df = pd.read_csv(file, header=0)
                            df.columns = df.columns.str.lower()

                        else:
                            # If no headers, use our expected headers
                            df = pd.read_csv(file, header=None, names=expected_headers)


                        df = self._clean_data(df)

                        vehicle_id = int(file.split('-')[-1].replace('.csv', ''))
                        df['vehicle_id'] = vehicle_id

                        dfs.append(self._enforce_required_columns(df))
                    except Exception as e:
                        print(f"Warning: Error processing file {file}: {str(e)}")
                        continue
            
            if not dfs:
                raise DataLoaderError("No valid data files were processed")
            
            self.df = pd.concat(dfs, ignore_index=True)
        except Exception as e:
            raise DataLoaderError(f"Error appending data: {str(e)}")