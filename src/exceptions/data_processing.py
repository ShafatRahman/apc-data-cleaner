class DataProcessorError(Exception):
    """Base exception class for data processing errors"""
    pass

class FilterError(DataProcessorError):
    """Exception raised for errors in filter operations"""
    pass