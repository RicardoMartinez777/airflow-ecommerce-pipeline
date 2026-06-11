# dags/etl_ecommerce.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator # Allows running Python functions as tasks

# DAG configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False, # False: each task execution is independent
    'email_on_failure': False, # Avoid sending emails if a task fails or retries
    'email_on_retry': False,
    'retries': 3,# Number of times a task will retry if it fails
    'retry_delay': timedelta(minutes=5), # Waiting time between retries
}
# Create DAG object (your pipeline)
dag = DAG(
    'etl_ecommerce',  # DAG name (unique ID)
    default_args=default_args, # Applies global configuration to all tasks
    description='Pipeline ETL de datos de e-commerce',# DAG description (informational only)
   
    
    start_date=datetime(2024, 1, 1),# Date when Airflow starts considering executions
    catchup=False,  # Avoid running past executions (important in real projects to prevent system overload)

    tags=['etl', 'ecommerce'],
)

# Task 1: Extract

def extract(**context):
    """Extract ecommerce data."""
    import pandas as pd
    
    print("📥 Extrayendo datos de ecommerce...")
    
    # In production, data would be read from Kafka, MQTT, or IoT APIs
    # For this project, data is read from local CSV files
    
    # Load data into memory to work with it inside the function
    orders = pd.read_csv('/opt/airflow/data/ecommerce_orders.csv')
    
    
    # Save paths for the next task using XCom to share information between tasks
    # note: For large datasets, pass file paths instead of DataFrames 
    context['ti'].xcom_push(key='orders_path',value='/opt/airflow/data/ecommerce_orders.csv')
   
    
    print(f"✅ Extraídas {len(orders)} órdenes")
    return True
# Connect the Python function with Airflow
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

# Task 2: Trasnform

def transform(**context):
    """Transform ecommerce data."""
    import pandas as pd
    
    print("🔄 Transformando datos de ecommerce...")
    
    # Get CSV path from the previous task
    orders_path = context['ti'].xcom_pull(key='orders_path',task_ids='extract')
    
    # Read data
    df = pd.read_csv(orders_path)
    
     # Convert date column
    df['order_date'] = pd.to_datetime(df['order_date'])

    # Create month column
    df['order_month'] = df['order_date'].dt.to_period('M').astype(str)
    
    # Calculate metrics
    metrics = {
        'total_orders': len(df),
        'total_revenue': float(df['total_amount'].sum()),
        'avg_order_value': float(df['total_amount'].mean())
    }
    
    # Save transformed data
    output_path = '/opt/airflow/output/orders_clean.parquet'
    df.to_parquet(output_path, index=False)
    
    context['ti'].xcom_push(key='metrics', value=metrics)
    context['ti'].xcom_push(key='output_path', value=output_path)
    
    print(f"✅ Transformadas {len(df)} órdenes")
    return True
# Connect the Python function with Airflow
transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

# Task 3: Load

def load(**context):
    """Load data into the destination."""
    import json
    
    print("💾 Cargando datos...")
    
    # Get metrics
    metrics = context['ti'].xcom_pull(key='metrics', task_ids='transform')
    output_path = context['ti'].xcom_pull(key='output_path', task_ids='transform')
    
    # In production, data would be loaded into a Data Warehouse
    # For this project, a summary file is generated
    summary = {
        # Logical pipeline date, not the actual execution time
        # Example: if the DAG runs today but processes yesterday's data,
        # execution_date represents = yesterday
        'execution_date': str(context['execution_date']),  
        'metrics': metrics,
        'output_file': output_path,
    }
    
    with open('/opt/airflow/output/summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"✅ Pipeline completado!")
    print(f"   Total orders: {metrics['total_orders']}")
    print(f"   Total revenue: {metrics['total_revenue']:,.2f}")
    
    return True

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)
# Define task execution order

extract_task >> transform_task >> load_task

# More explicit alternative:
# transform_task.set_upstream(extract_task)
# load_task.set_upstream(transform_task)