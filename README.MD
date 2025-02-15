# Automated Passenger Count (APC) Data Cleaner

## Project Structure

```
apc_processor/
├── data/
├── processor_outputs/
├── src/
│   ├── scripts/
│   ├── utils/
│   ├── tests/
│   ├── exceptions/
```

### `src/` 

Contains the source code for dataloaders, processors, error handlers, and tests. The apc_processor.py script is in the `scripts/` folder.

### `data/`

Contains the csv data files.

### `processor_outputs/`

Directory to store processor outputs.

## Setup instructions

### 1. Create and Activate Virtual Environment

```
python -m venv apc_venv
source apc_venv/bin/activate
```

### 2. Install Dependencies

```
pip install -r requirements.txt
```

### 3. Run Tests

```
python -m pytest -v
```


### 4. Running the APC Processor Script

The script accepts the following arguments:
- `folder_path`: Path to folder containing APC data files
- `start_date`: Start date in YYYY-MM-DDTHH:MM:SS format
- `end_date`: End date in YYYY-MM-DDTHH:MM:SS format
- `--output` or `-o`: Output file name (optional, defaults to results.csv)
- `--vehicle-ids` or `-v`: List of vehicle IDs to process (optional)

Example usage:

```
python -m src.scripts.apc_processor data/vehicles/clean-data 2024-04-01T00:00:00 2024-04-01T23:59:59 -o processor_outputs/clean-data-results.csv -v 120 121
```

```
python -m src.scripts.apc_processor data/vehicles/malformed-data 2024-04-01T00:00:00 2024-04-01T23:59:59 -o processor_outputs/malformed-data-results.csv -v 120 121
```

### Notes:
- For empty files, the script will show a warning and skip the file.
- I decided to change the start and end date inputs to apc_processor.py to be datetime formatted to enable filtering by time.
