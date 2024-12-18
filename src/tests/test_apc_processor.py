import pytest
import pandas as pd
import datetime
import os
from src.scripts.apc_processor import process_apc_data
from src.exceptions.data_loading import DataLoaderError


################### 
# 
# 
# Fixtures
#
#
################### 

@pytest.fixture
def sample_data_160():
    return pd.DataFrame({
        'date': ['2024-04-01', '2024-04-01'],
        'time': ['10:00:00', '11:00:00'],
        'ons': [5, 10],
        'offs': [3, 8],
        'latitude': [40.7128, 40.7129],
        'longitude': [-74.0060, -74.0061],
        'dwell time': [20, 25],
        'vehicle_id': [160, 160]
    })

@pytest.fixture
def sample_data_161():
    return pd.DataFrame({
        'date': ['2024-04-02'],
        'time': ['12:00:00'],
        'ons': [15],
        'offs': [12],
        'latitude': [40.7130],
        'longitude': [-74.0062],
        'dwell time': [30],
        'vehicle_id': [161]
    })

@pytest.fixture
def temp_csv_dir(sample_data_160, sample_data_161, tmp_path):
    # Create temporary CSV files
    csv_path_160 = tmp_path / "ridership-data-160.csv"
    csv_path_161 = tmp_path / "ridership-data-161.csv"
    
    sample_data_160.to_csv(csv_path_160, index=False)
    sample_data_161.to_csv(csv_path_161, index=False)
    
    return str(tmp_path)

def test_process_apc_with_date_filter(temp_csv_dir):
    start_date = datetime.datetime(2024, 4, 1, 9, 0, 0)
    end_date = datetime.datetime(2024, 4, 1, 23, 59, 59)
    output_file = "test_output.csv"
    
    try:
        result_file = process_apc_data(temp_csv_dir, start_date, end_date, output_file)
        assert os.path.exists(result_file)
        
        # Read and verify results
        result_df = pd.read_csv(result_file)
        assert len(result_df) == 2
        assert 'event_timestamp' in result_df.columns
        assert all(result_df['vehicle_id'] == 160)
    finally:
        # Cleanup
        if os.path.exists(output_file):
            os.remove(output_file)

def test_process_apc_with_time_filter(temp_csv_dir):
    start_time = datetime.datetime(2024, 4, 1, 10, 0, 0)
    end_time = datetime.datetime(2024, 4, 1, 10, 30, 0)
    output_file = "test_output.csv"
    
    try:
        result_file = process_apc_data(temp_csv_dir, start_time, end_time, output_file)
        assert os.path.exists(result_file)
        
        # Read and verify results
        result_df = pd.read_csv(result_file)
        assert len(result_df) == 1
        assert all(result_df['vehicle_id'] == 160)
    finally:
        # Cleanup
        if os.path.exists(output_file):
            os.remove(output_file)

def test_process_apc_data_with_vehicle_filter(temp_csv_dir):
    start_date = datetime.datetime(2024, 4, 1)
    end_date = datetime.datetime(2024, 4, 1, 23, 59, 59)
    output_file = "test_output.csv"
    
    try:
        result_file = process_apc_data(temp_csv_dir, start_date, end_date, output_file, [161])
        assert os.path.exists(result_file)
        
        # Read and verify results
        result_df = pd.read_csv(result_file)
        assert all(result_df['vehicle_id'] == 161)
    finally:
        # Cleanup
        if os.path.exists(output_file):
            os.remove(output_file)

@pytest.fixture
def repeated_headers_data(tmp_path):
    # Create data with repeated headers
    data = """
            date,time,ons,offs,latitude,longitude,dwell time
            date,time,ons,offs,latitude,longitude,dwell time
            date,time,ons,offs,latitude,longitude,dwell time
            2024-04-01,10:00:00,5,3,40.7128,-74.0060,20
            2024-04-01,11:00:00,10,8,40.7129,-74.0061,25
            date,time,ons,offs,latitude,longitude,dwell time
            2024-04-02,12:00:00,15,12,40.7130,-74.0062,30
            date,time,ons,offs,latitude,longitude,dwell time
            """
    
    csv_path = tmp_path / "ridership-data-160.csv"
    with open(csv_path, 'w') as f:
        f.write(data)
    return str(tmp_path)

def test_process_repeated_headers(repeated_headers_data):
    start_date = datetime.datetime(2024, 4, 1, 0, 0, 0)
    end_date = datetime.datetime(2024, 4, 2, 23, 59, 59)
    output_file = "test_output.csv"
    
    try:
        result_file = process_apc_data(repeated_headers_data, start_date, end_date, output_file)
        assert os.path.exists(result_file)
        
        # Read and verify results
        result_df = pd.read_csv(result_file)
        assert len(result_df) == 3  # Should have 3 data rows
        assert 'event_timestamp' in result_df.columns
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)

@pytest.fixture
def empty_file(tmp_path):
    csv_path = tmp_path / "ridership-data-161.csv"
    csv_path.touch()
    return str(tmp_path)

def test_process_empty_file(empty_file):
    start_date = datetime.datetime(2024, 4, 1, 0, 0, 0)
    end_date = datetime.datetime(2024, 4, 1, 23, 59, 59)
    output_file = "test_output.csv"
    
    with pytest.raises(DataLoaderError) as exc_info:
        process_apc_data(empty_file, start_date, end_date, output_file)
    assert "No valid data files were processed" in str(exc_info.value)


@pytest.fixture
def no_header_data(tmp_path):
    # Create data without headers
    data = """
            2024-04-01,10:00:00,5,3,40.7128,-74.0060,20
            2024-04-01,11:00:00,10,8,40.7129,-74.0061,25
            2024-04-02,12:00:00,15,12,40.7130,-74.0062,30
            """
    
    csv_path = tmp_path / "ridership-data-162.csv"
    with open(csv_path, 'w') as f:
        f.write(data)
    return str(tmp_path)

def test_process_no_header(no_header_data):
    start_date = datetime.datetime(2024, 4, 1, 0, 0, 0)
    end_date = datetime.datetime(2024, 4, 2, 23, 59, 59)
    output_file = "test_output.csv"
    
    try:
        result_file = process_apc_data(no_header_data, start_date, end_date, output_file)
        assert os.path.exists(result_file)
        
        # Read and verify results
        result_df = pd.read_csv(result_file)
        assert len(result_df) == 3
        assert all(col in result_df.columns for col in ['event_timestamp', 'ons', 'offs', 'latitude', 'longitude'])
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)