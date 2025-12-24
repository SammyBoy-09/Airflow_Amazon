# Customer ETL Pipeline with Apache Airflow

An automated ETL (Extract, Transform, Load) pipeline built with Apache Airflow for processing and cleaning customer data.

## Features

### Data Processing
- **Extract**: Reads customer data from Excel/CSV files
- **Transform**: Comprehensive data cleaning and feature engineering
- **Load**: Saves processed data to CSV, Excel, and PostgreSQL database

### Advanced Capabilities
- ✅ **Bulk Load Operations**: Configurable chunk size for efficient data loading (default: 1000 rows)
- ✅ **Incremental vs Full Load**: Switch between full table replacement or appending new records
- ✅ **Date Format Conversion**: Converts timestamps to human-readable YYYY-MM-DD format
- ✅ **Data Quality Checks**: Email validation, duplicate removal, missing value handling
- ✅ **Feature Engineering**: Customer segmentation, tenure calculation, spend normalization

## Data Transformations

1. **Empty/Whitespace Handling**: Converts empty strings to NaN
2. **Missing Data**: Fills missing names with "Unknown"
3. **Duplicate Removal**: Removes duplicate customers by email
4. **Email Validation**: Validates and filters invalid email formats
5. **Text Normalization**: Converts names to uppercase, trims spaces
6. **Date Processing**: Converts and sorts by last order date
7. **Age Handling**: Converts to numeric, fills missing with mean
8. **Order Aggregations**: Global and state-level order statistics
9. **Customer Segmentation**: REGULAR vs LOYAL customer classification
10. **Customer Tenure**: Calculates days since last order
11. **Date Features**: Extracts year, month, day from order dates
12. **Spend Normalization**: Min-max scaling for total_spend

## Project Structure

```
airflow-master/
├── dags/
│   ├── customer_etl_dag.py      # Main DAG definition
│   └── example_bash_dag.py      # Example DAG
├── scripts/
│   ├── Extract.py               # Data extraction logic
│   ├── Transform.py             # Data transformation logic
│   └── Load.py                  # Data loading logic
├── data/
│   ├── raw/                     # Source data files
│   ├── processed/               # Output files (CSV, Excel)
│   └── staging/                 # Temporary processing files
├── config/
│   └── etl.yaml                 # ETL configuration (optional)
├── Docker/
│   ├── docker-compose.yaml      # Docker services configuration
│   └── .env                     # Environment variables
└── logs/                        # Airflow task logs
```

## Prerequisites

- Docker Desktop
- Python 3.8+
- Git

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/SammyBoy-09/Airflow.git
cd Airflow
```

### 2. Start Airflow Services

```bash
cd Docker
docker-compose up -d
```

This will start:
- **PostgreSQL**: Database for Airflow metadata and processed data
- **Redis**: Message broker for task queuing
- **Airflow Webserver**: UI accessible at http://localhost:8080
- **Airflow Scheduler**: DAG scheduling and execution

### 3. Access Airflow UI

Open your browser and navigate to: http://localhost:8080

Default credentials (configure in `.env`):
- Username: `airflow`
- Password: `airflow`

### 4. Place Source Data

Put your customer data file in:
```
data/raw/customers_200.xlsx
```

## Usage

### Running the ETL Pipeline

#### Manual Trigger
1. Go to http://localhost:8080
2. Find the `customer_etl` DAG
3. Click the play button to trigger manually

#### CLI Trigger
```bash
docker exec docker-webserver-1 airflow dags trigger customer_etl
```

#### Scheduled Runs
The DAG runs automatically on a daily schedule at midnight.

### Configuration Options

#### Load Type (Full vs Incremental)
In `dags/customer_etl_dag.py`, line 69:

```python
load_type="full"        # Replaces entire table
# OR
load_type="incremental" # Appends new records
```

#### Bulk Load Chunk Size
Adjust the batch size for loading:

```python
bulk_chunk_size=1000  # Load 1000 rows at a time
bulk_chunk_size=500   # Smaller batches
bulk_chunk_size=5000  # Larger batches
```

### Checking Task Status

```bash
docker exec docker-webserver-1 airflow tasks states-for-dag-run customer_etl <run_id>
```

### Viewing Logs

Logs are stored in:
```
logs/dag_id=customer_etl/
```

Or view in the Airflow UI under each task.

## Output Files

After successful execution:

- **CSV**: `data/processed/cleaned_data.csv`
- **Excel**: `data/processed/cleaned_data.xlsx`
- **PostgreSQL Table**: `customers_cleaned`
- **Order Summary**: `data/processed/orders_summary.csv` & `.xlsx`

## Database Connection

The pipeline connects to PostgreSQL:

```
Host: postgres (Docker network)
Port: 5432
Database: airflow
Username: airflow
Password: airflow
```

Access via pgAdmin or CLI:
```bash
docker exec -it docker-postgres-1 psql -U airflow -d airflow
```

Query the data:
```sql
SELECT * FROM customers_cleaned LIMIT 10;
```

## Future Enhancements

### Smart Incremental Loading (Option 2)
Currently documented in `scripts/Load.py` as TODO:

- Metadata table for tracking last load timestamps
- Date-based filtering for new/updated records only
- Automatic first-run detection
- Estimated effort: ~30-35 lines across 3 files

## Troubleshooting

### Containers Not Starting
```bash
docker-compose down
docker-compose up -d
```

### DAG Not Appearing
- Check for syntax errors in `dags/customer_etl_dag.py`
- Verify the DAG is in the `/opt/airflow/dags` folder
- Wait 30 seconds for scheduler to parse DAGs

### Task Failures
- Check logs in Airflow UI
- Verify source data file exists: `data/raw/customers_200.xlsx`
- Check PostgreSQL connection

### Permission Issues
```bash
sudo chown -R $(id -u):$(id -g) .
```

## Technologies Used

- **Apache Airflow 2.8.3**: Workflow orchestration
- **PostgreSQL 15**: Data storage
- **Redis 7**: Task queue
- **Pandas**: Data manipulation
- **SQLAlchemy**: Database ORM
- **Docker**: Containerization
- **Python 3.8**: Core language

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is open source and available for educational purposes.

## Contact

For questions or issues, please open a GitHub issue.

---

**Last Updated**: December 2025
