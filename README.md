# ETL Pipeline Framework

A production-ready ETL (Extract, Transform, Load) pipeline framework with config-driven data cleaning, built on Apache Airflow and Docker.

## 📋 Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup Instructions](#detailed-setup-instructions)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Components](#components)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## ✨ Features

### Core ETL Capabilities
- ✅ **Extract**: Read data from CSV, Excel, and databases
- ✅ **Transform**: Config-driven data cleaning and transformation
- ✅ **Load**: Save to CSV, Excel, and PostgreSQL databases

### Data Cleaning Features
- ✅ Trim whitespace from string columns
- ✅ Handle missing data (mean, median, mode, forward fill, backward fill, drop)
- ✅ Remove duplicate records
- ✅ Validate data formats (email, dates, etc.)
- ✅ Type casting and conversion
- ✅ Outlier detection and removal (IQR method)
- ✅ Empty string detection and handling
- ✅ Config-driven cleaning rules via YAML

### Additional Features
- ✅ Pydantic data validation
- ✅ SQLAlchemy database connectivity
- ✅ Modular and reusable utilities
- ✅ Apache Airflow integration
- ✅ Docker support for containerized deployment

## 📁 Project Structure

```
airflow-master/
├── config/                      # Configuration files
│   ├── cleaning_rules.yaml      # Data cleaning rules
│   └── etl_config.yaml          # ETL pipeline configuration
├── dags/                        # Airflow DAG definitions
│   └── customer_etl_dag.py      # Customer ETL pipeline DAG
├── data/                        # Data directories
│   ├── raw/                     # Input data
│   ├── staging/                 # Intermediate data
│   └── processed/               # Output data
├── scripts/                     # ETL scripts and utilities
│   ├── Extract.py               # Data extraction module
│   ├── Transform.py             # Data transformation module
│   ├── Transform_enhanced.py    # Enhanced config-driven transform
│   ├── Load.py                  # Data loading module
│   ├── cleaning_utils.py        # Reusable cleaning utilities
│   └── config_loader.py         # Configuration loader
├── Docker/                      # Docker configuration
│   └── docker-compose.yaml      # Docker Compose setup
├── requirements.txt             # Python dependencies
├── demo_etl.py                  # Demo script
└── README.md                    # This file
```

## 📦 Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.10 or 3.11** (required for local development)
- **Docker Desktop** (required for Airflow deployment)
- **Git** (for version control)
- **PowerShell** or **Command Prompt** (Windows)
- At least **4GB RAM** available for Docker containers

### Check Prerequisites

```powershell
# Check Python version
python --version

# Check Docker installation
docker --version
docker-compose --version

# Check Git installation
git --version
```

## 🚀 Quick Start

### Step 1: Clone the Repository

```powershell
git clone <your-repository-url>
cd airflow-master
```

### Step 2: Setup Python Virtual Environment

```powershell
# Create virtual environment with Python 3.10
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Start Docker Services

```powershell
# Navigate to Docker directory
cd Docker

# Start all services (first time setup takes 2-3 minutes)
docker-compose up -d

# Check if all services are running
docker-compose ps
```

### Step 4: Access Airflow UI

1. Open browser and go to: **http://localhost:8080**
2. Login with credentials:
   - **Username**: `airflow`
   - **Password**: `airflow`
3. Find the `customer_etl` DAG in the DAG list
4. Toggle it ON (switch button)
5. Click the Play button to trigger the DAG

### Step 5: Verify Data Output

```powershell
# View cleaned data in CSV
cd ..
Get-Content data\processed\cleaned_data.csv | Select-Object -First 10

# Or check PostgreSQL database
cd Docker
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM customers_cleaned LIMIT 5;"
```

## 📚 Detailed Setup Instructions

### Part A: Local Development Environment

#### 1. Create Python Virtual Environment

**Option 1: Using venv (Recommended)**

```powershell
# Navigate to project directory
cd airflow-master

# Create virtual environment
python -m venv venv

# Activate virtual environment (PowerShell)
.\venv\Scripts\Activate.ps1

# For Command Prompt use:
# .\venv\Scripts\activate.bat

# For Git Bash/Linux/Mac use:
# source venv/bin/activate

# Verify activation (you should see (venv) in prompt)
```

**Option 2: Using conda**

```powershell
# Create conda environment with Python 3.10
conda create -n etl-pipeline python=3.10

# Activate environment
conda activate etl-pipeline

# Verify activation
python --version
```

#### 2. Install Python Dependencies

```powershell
# Make sure virtual environment is activated
pip install --upgrade pip

# Install all dependencies
pip install -r requirements.txt

# Verify installation
pip list

# Check specific packages
pip show pandas pydantic sqlalchemy
```

#### 3. Run Unit Tests

```powershell
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=scripts --cov-report=term-missing

# Expected output: 16 tests passed
```

### Part B: Docker Deployment (Airflow)

#### 1. Understand Docker Setup

The project uses Docker Compose to run:
- **PostgreSQL**: Database for Airflow metadata and data storage
- **Redis**: Message broker for task queuing
- **Airflow Webserver**: Web UI (port 8080)
- **Airflow Scheduler**: DAG scheduler and executor

#### 2. Configure Environment Variables

The Docker setup uses `.env` file for configuration:

```powershell
# Navigate to Docker directory
cd Docker

# View environment configuration (optional)
Get-Content .env
```

Key variables:
- `AIRFLOW_UID`: User ID for Airflow processes
- `POSTGRES_USER`, `POSTGRES_PASSWORD`: Database credentials
- `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`: Webserver login

#### 3. Start Docker Services

```powershell
# First time setup (initializes database and creates admin user)
docker-compose up -d

# Check service status
docker-compose ps

# Expected output: 4 services running
# - postgres (healthy)
# - redis (healthy)
# - webserver (healthy)
# - scheduler (up)

# View logs (optional)
docker-compose logs -f webserver
docker-compose logs -f scheduler
```

#### 4. Access Airflow Web UI

1. **Open Browser**: Navigate to http://localhost:8080
2. **Login**: 
   - Username: `airflow`
   - Password: `airflow`
3. **Verify DAGs**: You should see `customer_etl` DAG in the list

#### 5. Trigger Your First DAG Run

1. Find `customer_etl` in the DAG list
2. Toggle the DAG to **ON** (switch on the left)
3. Click the **Play button** (▶) on the right
4. Select **Trigger DAG**
5. Monitor execution in the **Graph** or **Grid** view

#### 6. Monitor Execution

**View Task Logs:**
- Click on a task box in the Graph view
- Click **Logs** button
- Check for any errors or success messages

**Check Task Status:**
- Green = Success
- Red = Failed
- Yellow = Running
- Light blue = Queued

#### 7. Stop Docker Services

```powershell
# Stop services (data persists)
docker-compose stop

# Stop and remove containers (data persists in volumes)
docker-compose down

# Stop and remove everything including data
docker-compose down -v
```

## ⚙️ Configuration

### ETL Configuration (`config/etl_config.yaml`)

```yaml
data_source:
  input_path: "data/raw/customers_200.xlsx"
  input_format: "excel"

data_destination:
  output_path: "data/processed/cleaned_data.csv"
  output_formats:
    - csv
    - xlsx

database:
  type: "postgresql"
  host: "postgres"
  port: 5432
  database: "airflow"
  table_name: "customers_cleaned"
```

### Cleaning Rules (`config/cleaning_rules.yaml`)

```yaml
columns:
  name:
    required: true
    trim: true
    uppercase: true
    
  email:
    required: true
    validate_format: true
    regex_pattern: '^[\w\.-]+@[\w\.-]+\.\w+$'
    
  age:
    data_type: int
    fill_strategy: mean
    min_value: 0
    max_value: 120

global:
  remove_duplicates:
    enabled: true
    subset: ['email']
    keep: 'first'
```

## 📖 Usage

### 1. Config Loader

```python
from scripts.config_loader import ConfigLoader

# Load ETL configuration
config = ConfigLoader.load_etl_config("config/etl_config.yaml")

# Load cleaning rules
rules = ConfigLoader.load_cleaning_rules("config/cleaning_rules.yaml")
```

### 2. Cleaning Utilities

```python
from scripts.cleaning_utils import DataCleaner
import pandas as pd

df = pd.read_csv("data/raw/input.csv")

# Trim whitespace
df = DataCleaner.trim_whitespace(df)

# Fill missing values
df = DataCleaner.fill_missing_mean(df, ['age', 'salary'])

# Remove duplicates
df = DataCleaner.remove_duplicates(df, subset=['email'])

# Validate emails
df = DataCleaner.validate_email(df, 'email', drop_invalid=True)

# Type casting
df = DataCleaner.typecast_column(df, 'age', 'int')
```

### 3. Complete ETL Pipeline

```python
from scripts.Extract import extract
from scripts.Transform_enhanced import transform
from scripts.Load import load

# Extract
df = extract("data/raw/customers.xlsx")

# Transform (config-driven)
df_cleaned = transform(df, use_config=True)

# Load
load(df_cleaned, 
     csv_path="data/processed/output.csv",
     xlsx_path="data/processed/output.xlsx")
```

### 4. Demo Script

Run the comprehensive demo to see all features:

```powershell
python demo_etl.py
```

The demo includes:
- Basic cleaning operations
- Missing data handling strategies
- CSV read/write operations
- Config-driven cleaning
- Data validation

## 🧩 Components

### 1. Config Loader (`config_loader.py`)
- Load YAML and JSON configuration files
- Pydantic validation for configurations
- Type-safe configuration objects

### 2. Cleaning Utilities (`cleaning_utils.py`)
- **DataCleaner class** with 20+ cleaning methods:
  - `trim_whitespace()` - Remove leading/trailing spaces
  - `fill_missing_mean()` - Fill with mean
  - `fill_missing_median()` - Fill with median
  - `fill_missing_mode()` - Fill with most frequent value
  - `fill_missing_regression()` - Predictive imputation
  - `remove_duplicates()` - Remove duplicate rows
  - `typecast_column()` - Convert data types
  - `validate_email()` - Email format validation
  - `remove_outliers_iqr()` - Outlier removal
  - And more...

### 3. Extract Module (`Extract.py`)
- Read CSV files
- Read Excel files (auto-convert to CSV)
- Extensible for database sources

### 4. Transform Module (`Transform.py` / `Transform_enhanced.py`)
- Default hardcoded transformation rules
- Config-driven transformation (enhanced version)
- Applies cleaning rules from YAML configuration
- Comprehensive logging

### 5. Load Module (`Load.py`)
- Save to CSV
- Save to Excel
- Save to PostgreSQL database
- Configurable output formats

## 🧪 Testing

### Test Individual Modules

```powershell
# Test config loader
python scripts/config_loader.py

# Test cleaning utilities
python scripts/cleaning_utils.py

# Test transform module
python scripts/Transform_enhanced.py
```

### Run All Tests

```powershell
pytest tests/
```

## 📦 Core Dependencies

```
pandas>=2.0.0          # Data manipulation
numpy>=1.24.0          # Numerical computing
pydantic>=2.0.0        # Data validation
sqlalchemy>=2.0.0      # Database ORM
psycopg2-binary        # PostgreSQL driver
PyYAML>=6.0            # YAML parsing
openpyxl>=3.1.0        # Excel support
apache-airflow>=2.8.0  # Workflow orchestration
```

## 🎯 Task Completion Checklist

- ✅ Python virtual environment setup (venv/conda)
- ✅ Folder structure for ETL pipeline framework
- ✅ Core libraries installed (pandas, pydantic, sqlalchemy)
- ✅ Config loader (YAML/JSON) implemented
- ✅ Demo script for CSV read/write operations
- ✅ Reusable cleaning utilities (trim, fillna, typecast)
- ✅ Incorrect data type handling
- ✅ Duplicate data detection & removal
- ✅ Missing data handling strategies (mean, median, mode, regression, drop)
- ✅ Config-driven cleaning rules via YAML

## 📝 Usage Examples

### Example 1: Quick Start
```python
import pandas as pd
from scripts.cleaning_utils import DataCleaner

# Load data
df = pd.read_csv("data.csv")

# Clean data
df = DataCleaner.trim_whitespace(df)
df = DataCleaner.remove_duplicates(df)
df = DataCleaner.fill_missing_mean(df, ['age'])

# Save
df.to_csv("cleaned_data.csv", index=False)
```

### Example 2: Config-Driven Approach
```python
from scripts.config_loader import ConfigLoader
from scripts.Transform_enhanced import transform

# Load data
df = pd.read_csv("data.csv")

# Apply config-driven cleaning
df_cleaned = transform(df, use_config=True)

# Save
df_cleaned.to_csv("cleaned_data.csv", index=False)
```

## 🔧 Troubleshooting

### Common Issues and Solutions

#### Issue 1: Docker Containers Not Starting

```powershell
# Check Docker Desktop is running
docker --version

# Check for port conflicts (8080, 5432, 6379)
netstat -ano | findstr "8080"

# Restart Docker services
cd Docker
docker-compose down
docker-compose up -d
```

#### Issue 2: Airflow Login Not Working

```powershell
# Create new user if needed
docker-compose exec webserver airflow users create \
    --username airflow \
    --firstname Air \
    --lastname Flow \
    --role Admin \
    --email admin@example.com \
    --password airflow

# List all users
docker-compose exec webserver airflow users list
```

#### Issue 3: DAG Not Appearing in UI

```powershell
# Check DAG file syntax
docker-compose exec webserver python /opt/airflow/dags/customer_etl_dag.py

# View scheduler logs
docker-compose logs scheduler

# Restart scheduler
docker-compose restart scheduler
```

#### Issue 4: Transform Task Failing

```powershell
# Check task logs in Airflow UI
# Or view logs directly
docker-compose exec webserver cat /opt/airflow/logs/dag_id=customer_etl/run_id=<run-id>/task_id=transform_data/attempt=1.log

# Verify cleaning_utils.py is accessible
docker-compose exec webserver ls /opt/airflow/scripts/

# Restart services to pick up code changes
docker-compose restart webserver scheduler
```

#### Issue 5: Python Virtual Environment Issues

```powershell
# Deactivate current environment
deactivate

# Remove old environment
Remove-Item -Recurse -Force venv

# Create fresh environment
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

#### Issue 6: Port Already in Use

```powershell
# Find process using port 8080
netstat -ano | findstr "8080"

# Kill process (replace <PID> with actual process ID)
taskkill /PID <PID> /F

# Or change Airflow port in docker-compose.yaml
# Change "8080:8080" to "8081:8080"
```

### Helpful Commands

```powershell
# View all running containers
docker ps

# View container logs
docker-compose logs -f <service-name>

# Execute command in container
docker-compose exec <service-name> <command>

# Restart specific service
docker-compose restart <service-name>

# Check container resource usage
docker stats

# Clean up Docker resources
docker system prune -a
```

## 🎓 Learning Resources

### Apache Airflow
- [Official Documentation](https://airflow.apache.org/docs/)
- [Airflow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

### Pandas
- [Official Documentation](https://pandas.pydata.org/docs/)
- [10 Minutes to pandas](https://pandas.pydata.org/docs/user_guide/10min.html)

### Docker
- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Run tests (`pytest tests/`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👥 Authors

- **Data Engineering Team** - Initial work

## 📞 Support

For issues and questions:
- Create an issue in the GitHub repository
- Check the [Troubleshooting](#troubleshooting) section
- Review Airflow logs for error details

## 🙏 Acknowledgments

- Apache Airflow community for the excellent orchestration platform
- pandas development team for powerful data manipulation tools
- pydantic contributors for data validation framework
- Docker community for containerization tools
