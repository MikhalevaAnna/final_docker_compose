from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from clickhouse_connect.driver import create_client
import csv, os, pendulum, random
from datetime import datetime, timedelta
import psycopg2
from pyspark.sql.functions import col
from pyspark.sql import SparkSession


connection_to_postgres = None
connection_to_clickhouse = None
cursor = None
os.makedirs("sample_data/", exist_ok=True)
file_name = "sample_data/sales_data.csv"
num_records = 1000000
table_name_full = "sales_data"
table_name_agg = "sales_data_agg"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}


dag = DAG(
    'main',
    default_args=default_args,
    description='A simple DAG to interact with PySpark, PostgreSQL and ClickHouse',
    catchup=False,
    start_date=pendulum.datetime(2025, 5, 31, tz="Europe/Moscow"),
    schedule_interval='45 12 * * 2',
)


try:
     connection_to_postgres = psycopg2.connect(
         host="host.docker.internal",
         port="5432",
         database="test",
         user="user",
         password="password"
     )
     cursor = connection_to_postgres.cursor()

     connection_to_clickhouse = create_client(
         host='host.docker.internal',
         port=8123,
         username='',
         password=''
     )
except Exception as error:
    raise Exception(f'Подключиться к БД не удалось! Ошибка: {error}!')


def get_random_date():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    return (start_date + (end_date - start_date) * random.random()).strftime('%Y-%m-%d')


def generate_file():
    regions_name = ['North', 'South', 'East', 'West']
    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['sale_id', 'customer_id', 'product_id', 'quantity',
                         'sale_date', 'sale_amount', 'region'])
        for i in range(num_records):
            sale_id = i + 1
            customer_id = random.randint(1, 7000)
            product_id = random.randint(1, 200)
            quantity = random.randint(1, 10)
            sale_date = get_random_date()
            sale_amount = round(quantity * (random.random() + random.randint(5, 300)), 2)
            region = random.choice(regions_name)

            writer.writerow([sale_id, customer_id, product_id, quantity,
                             sale_date, sale_amount, region])
    print(f"Сгенерировано {num_records} записей и сохранено в {file_name}.")


def migration_from_spark_to_postgres():
    spark = SparkSession.builder \
        .appName("Airflow_PySpark") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
        .master("local") \
        .getOrCreate()
    df = spark.read.csv(file_name, header=True, inferSchema=True)
    df_date = (df.withColumn("sale_id", col("sale_id").cast('integer'))
                 .withColumn("customer_id", col("customer_id").cast('integer'))
                 .withColumn("product_id", col("product_id").cast('integer'))
                 .withColumn("quantity", col("quantity").cast('smallint'))
                 .withColumn("sale_date",col("sale_date").cast('date'))
                 .withColumn("sale_amount",col("sale_amount").cast('float'))
                 .withColumn("region",col("region").cast('string'))
                 )
    df_date.printSchema()
    count_total = df_date.count()
    print(f"Количество записей в датафрейме всего: {count_total}.")
    df_data_clear = df_date.distinct()
    count_clear = df_data_clear.count()
    print(f"Количество записей в датафрейме всего без дубляжа: {count_clear}.")
    df_data_clear.show(5)
    df_data_clear.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
        .option("dbtable", "sales_data") \
        .option("user", "user") \
        .option("password", "password") \
        .mode("overwrite") \
        .save()
    spark.stop()


def aggregation_in_postgres():
    try:
        cursor.execute(f"""DROP TABLE IF EXISTS {table_name_agg};""")
        connection_to_postgres.commit()
        cursor.execute(f"""CREATE TABLE {table_name_agg} AS 
             SELECT  
                   region
                   , product_id
                   , count(*) AS total_quantity
                   , (sum(sale_amount :: float)) AS total_sale_amount
                   , (avg(sale_amount)) :: float AS average_sale_amount
             FROM {table_name_full}      
             GROUP BY region, product_id 
             ORDER BY region, product_id
        ;""")
        connection_to_postgres.commit()
        print(f"Агрегированные данные успешно загружены в PostgreSQL в таблицу {table_name_agg}!")
    except Exception as error:
        connection_to_postgres.rollback()
        raise Exception(f'Загрузить агрегированные данные в PostgreSQL в таблицу {table_name_agg} не удалось! '
                        f'Ошибка: {error}!')


def migration_from_postgres_to_clickhouse():
    try:
        connection_to_clickhouse.command(f"""
                        DROP TABLE IF EXISTS {table_name_agg};""")
        connection_to_clickhouse.command(f"""
                        CREATE TABLE {table_name_agg} (
                            region String,
                            product_id UInt32,
                            total_quantity UInt32,
                            total_sale_amount Decimal(10, 2),
                            average_sale_amount Decimal(10, 2),
                            update_date DateTime DEFAULT now()
                        ) ENGINE = MergeTree()
                        ORDER BY (update_date, product_id)
            ;""")

        cursor.execute(f"SELECT * FROM {table_name_agg};")
        rows = cursor.fetchall()
        connection_to_clickhouse.insert(table_name_agg, rows,
                                 column_names=['region', 'product_id', 'total_quantity'
                                             , 'total_sale_amount', 'average_sale_amount'])
        print(f"Агрегированные данные успешно загружены в Clickhouse в таблицу {table_name_agg}!")
    except Exception as error:
        raise Exception(
            f'Загрузить агрегированные данные в Clickhouse в таблицу {table_name_agg} не удалось! '
            f'Ошибка: {error}!')
    finally:
        if cursor:
            cursor.close()
        if connection_to_postgres:
            connection_to_postgres.close()


task_generate_file = PythonOperator(
    task_id='generate_file',
    python_callable=generate_file,
    dag=dag,
)


task_migration_from_spark_to_postgres = PythonOperator(
    task_id='migration_from_spark_to_postgres',
    python_callable=migration_from_spark_to_postgres,
    dag=dag,
)


task_aggregation_in_postgres = PythonOperator(
    task_id='aggregation_in_postgres',
    python_callable=aggregation_in_postgres,
    dag=dag,
)


task_migration_from_postgres_to_clickhouse = PythonOperator(
    task_id='migration_from_postgres_to_clickhouse',
    python_callable=migration_from_postgres_to_clickhouse,
    dag=dag,
)


(
    task_generate_file >>
    task_migration_from_spark_to_postgres >>
    task_aggregation_in_postgres >>
    task_migration_from_postgres_to_clickhouse
 )
