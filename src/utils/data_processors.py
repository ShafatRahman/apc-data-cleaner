import pandas as pd
import datetime
from typing import Optional
from src.exceptions.data_processing import DataProcessorError, FilterError

class BaseDataProcessor:
    def __init__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame")
        self.df = df
        self.filter = pd.Series(True, index=self.df.index)

    def apply_filters(self) -> pd.DataFrame:
        try:
            return self.df[self.filter]
        except Exception as e:
            raise FilterError(f"Error applying filters: {str(e)}")

    def clear_filters(self):
        try:
            self.filter = pd.Series(True, index=self.df.index)
        except Exception as e:
            raise FilterError(f"Error clearing filters: {str(e)}")

    def add_filter(self, mask: pd.Series):
        if not isinstance(mask, pd.Series):
            raise TypeError("Filter mask must be a pandas Series")
        if len(mask) != len(self.filter):
            raise ValueError("Filter mask length must match DataFrame length")
        try:
            self.filter &= mask
            return self
        except Exception as e:
            raise FilterError(f"Error adding filter: {str(e)}")

class APCDataProcessor(BaseDataProcessor):
    def __init__(self, df: pd.DataFrame):
        super().__init__(df)

    def filter_by_date_and_time(self, start_datetime: datetime.datetime, end_datetime: datetime.datetime):
        try:
            if 'event_timestamp' not in self.df.columns:
                raise KeyError("DataFrame missing required 'event_timestamp' column")
            
            mask = (self.df['event_timestamp'] >= start_datetime.isoformat()) & \
                   (self.df['event_timestamp'] <= end_datetime.isoformat())
            return self.add_filter(mask)
        except Exception as e:
            raise FilterError(f"Error applying date filter: {str(e)}")

    def filter_by_vehicle(self, vehicle_ids: set[int] = None):
        try:
            if 'vehicle_id' not in self.df.columns:
                raise KeyError("DataFrame missing required 'vehicle_id' column")
            
            if vehicle_ids:
                mask = self.df['vehicle_id'].isin(vehicle_ids)
                return self.add_filter(mask)
            return self
        except Exception as e:
            raise FilterError(f"Error applying vehicle filter: {str(e)}")
    
    def process(self) -> pd.DataFrame:
        try:
            return self.apply_filters()
        except Exception as e:
            raise DataProcessorError(f"Error processing data: {str(e)}")
