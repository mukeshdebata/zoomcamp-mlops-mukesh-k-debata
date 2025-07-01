# MLOps Zoomcamp 2025 - Module 5: Model Monitoring

## Overview

This module focuses on implementing model monitoring for ML batch services using:
- **PostgreSQL**: For storing monitoring metrics
- **Grafana**: For visualizing metrics
- **Evidently**: For calculating data quality and model performance metrics
- **Docker**: For containerizing the monitoring stack

The homework for this module focuses on setting up a monitoring system for a taxi trip duration prediction service. We use real NYC Green Taxi data from March 2024 to simulate production data and implement monitoring to track data drift and model performance metrics over time.

## Project Structure

- `docker-compose.yml`: Configuration for PostgreSQL, Adminer, and Grafana services
- `evidently_metrics_calculation.py`: Script for calculating metrics and storing them in PostgreSQL
- `homework05.ipynb`: Jupyter notebook containing homework solutions
- `homework05.md`: Homework questions and tasks
- `/config/`: Configuration files for Grafana datasources and dashboards
- `/dashboards/`: Store for Grafana dashboard configurations
- `/data/`: Contains taxi trip data used for model training and monitoring
- `/models/`: Contains the trained linear regression model

## Homework Summary and Solutions

### Question 1: Prepare the dataset
- Downloaded March 2024 Green Taxi data using NYC TLC open data API
- Processed the data by creating a target variable `duration_min` calculated from pickup and dropoff timestamps
- Applied data cleaning by filtering for reasonable trip durations (0-60 minutes) and passenger counts (1-8)
- After pre-processing and cleaning, the dataset contained 57,457 rows (this was the correct answer for Q1)
- Split data into training (first 30,000 rows) and validation sets for model building
- Built a simple Linear Regression model using passenger count, trip distance, fare amount, total amount, pickup and dropoff locations as features
- Evaluated model performance using Mean Absolute Error on both training and validation sets
- Saved the trained model to `models/lin_reg.bin` and reference data to `data/reference.parquet` for monitoring

### Question 2: Metrics
- Expanded the monitoring metrics beyond basic data drift checks
- Added additional metrics for monitoring:
  - `QuantileValue` metric for the "fare_amount" column with quantile=0.5 (median fare value)
  - `ValueDrift` metric for the "prediction" column to track changes in model predictions
- Used Evidently's DataDefinition to define numerical and categorical columns
- Created separate datasets for train and validation data
- Built an Evidently Report with the selected metrics and test inclusions
- Ran the report to calculate metric values between reference and current data
- Extracted and displayed specific metric values from the report results
- The combination of these metrics allows monitoring both data characteristics (fare amount distribution) and model behavior (prediction drift)

![Evidently report in notebook](./images/evidently_notebook.PNG)

### Question 3: Monitoring
- Set up a PostgreSQL database to store metrics using Docker
- Created a table "dummy_metrics" with columns for timestamp, quantile_fare, and pred_drift
- Developed a data pipeline in `evidently_metrics_calculation.py` that:
  - Establishes a connection to PostgreSQL database
  - Creates necessary database tables
  - Loads the reference data and trained model
  - Processes the March 2024 data day by day
  - For each day, calculates drift metrics between that day's data and the reference data
  - Stores calculated metrics in the PostgreSQL database with appropriate timestamps
- Implemented Prefect tasks to orchestrate database preparation and metrics calculation
- Executed the monitoring pipeline and analyzed the results
- Queried the database to find that the maximum value of the quantile=0.5 metric for "fare_amount" was 14.2 (this was the correct answer for Q3)
- This approach allows tracking how metrics evolve over time, enabling detection of gradual drift

![Prefect batch running](./images/prefect_batch_running.PNG)

![PostgreSQL database with metrics](./images/postgres_db_image.PNG)

### Question 4: Dashboard
- Set up Grafana for visualization of monitoring metrics using Docker
- Configured Grafana to connect to PostgreSQL as a data source
- Created custom Grafana dashboard panels showing:
  - Time series of fare amount median (quantile=0.5) values
  - Time series of prediction drift values
  - Alerts for when metrics exceed certain thresholds
- Customized dashboard visualization settings including colors, thresholds, and legends
- Added titles, descriptions, and proper labeling to make the dashboard informative
- Saved the complete dashboard configuration as a JSON file
- Stored the dashboard config file in the `dashboards` directory (this was the correct answer for Q4)
- This configuration enables easy deployment of the same dashboard in different environments and preserves the visualization setup

![Grafana Dashboard](./images/grafana_hw05.PNG)

## How to Run the Monitoring Stack

1. **Start the services**:
   ```bash
   docker-compose up -d
   ```
   This starts PostgreSQL, Adminer, and Grafana services as defined in docker-compose.yml.

2. **Initialize the database and calculate metrics**:
   ```bash
   python evidently_metrics_calculation.py
   ```
   This script:
   - Creates the required database and tables
   - Loads the reference data and model
   - Processes March 2024 data day by day
   - Calculates and stores metrics in the database

3. **Access Grafana dashboard**:
   - Open a browser and navigate to http://localhost:3000
   - Default credentials are admin/admin
   - Navigate to the pre-configured dashboard to visualize metrics

4. **Access database via Adminer**:
   - Open a browser and navigate to http://localhost:8080
   - Use the following credentials:
     - System: PostgreSQL
     - Server: db
     - Username: postgres
     - Password: example
     - Database: test

## Best Practices for Model Monitoring

- **Regular monitoring**: Check model performance metrics regularly
- **Set up alerts**: Configure alerts in Grafana for metric thresholds
- **Data quality checks**: Monitor for data drift and quality issues
- **Visualization**: Create dashboards that present metrics clearly
- **Historical tracking**: Store metrics over time to identify trends
- **Correlation analysis**: Analyze relationships between data drift and model performance
- **Incremental updates**: Use monitoring insights to determine when model retraining is necessary
- **Documentation**: Keep records of monitoring setup and alert resolutions
- **End-to-end testing**: Regularly verify that the entire monitoring pipeline is functioning correctly

## References

- Evidently AI documentation: https://docs.evidentlyai.com/
- Grafana documentation: https://grafana.com/docs/
- PostgreSQL documentation: https://www.postgresql.org/docs/
