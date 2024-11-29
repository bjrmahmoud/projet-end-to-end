first version of spark_stream.py

import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Fonction pour créer un keyspace Cassandra
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

# Fonction pour créer une table dans Cassandra
def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    print("Table created successfully!")

# Fonction pour insérer des données dans la table Cassandra
def insert_data(session, **kwargs):
    print("inserting data...")

    # Récupération des paramètres passés dans kwargs
    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        # Insertion des données dans la table Cassandra
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

# Fonction pour créer une connexion Spark
def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        # Configuration pour ignorer les logs non nécessaires
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

# Fonction pour connecter Kafka à Spark
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        # Connexion au topic Kafka 'users_created'
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df

# Fonction pour créer une connexion Cassandra
def create_cassandra_connection():
    try:
        # Connexion au cluster Cassandra local
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

# Fonction pour définir la structure des données et extraire les informations de Kafka
def create_selection_df_from_kafka(spark_df):
    # Définition du schéma des données (attend des données JSON avec des champs spécifiques)
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Extraction des données du flux Kafka, conversion de JSON à DataFrame structuré
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

# Point d'entrée principal
if __name__ == "__main__":
    # Création de la connexion Spark
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connexion à Kafka via Spark
        spark_df = connect_to_kafka(spark_conn)

        # Sélection des données depuis Kafka et conversion en DataFrame structuré
        selection_df = create_selection_df_from_kafka(spark_df)

        # Création de la connexion Cassandra
        session = create_cassandra_connection()

        if session is not None:
            # Création du keyspace et de la table dans Cassandra
            create_keyspace(session)
            create_table(session)
	    #insert_data(session)

            logging.info("Streaming is being started...")

            # Définition du flux de données en temps réel vers Cassandra
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')  # emplacement pour le checkpoint
                               .option('keyspace', 'spark_streams')  # keyspace dans Cassandra
                               .option('table', 'created_users')  # table dans le keyspace
                               .start())

            # Attente de l'achèvement de la tâche de streaming
            streaming_query.awaitTermination()

pour script ouvrir enrtypoint with terminal and run wsl chmod +x enrtypoint.sh
[
docker run -it --entrypoint bash projet-end-to-end1-webserver-1

airflow db init
airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@example.com --password admin
]
run docker compose up -d 


****Check Topic Creation (Optional)

After creating the topic, you can verify its existence:

docker exec -it broker kafka-topics --list --bootstrap-server broker:29092

This should list users_created among other topics in the cluster.

*** Create the users_created Topic Manually

Use the Kafka CLI inside the broker container to create the topic. Run this command in your terminal:

docker exec -it broker kafka-topics --create --topic users_created --bootstrap-server broker:29092 --partitions 1 --replication-factor 1

then run docker compose up -d 

ouvre airflow  http://localhost:8080/home

the user and passrword admin admin 
run the script of the dags 

docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042

CREATE KEYSPACE IF NOT EXISTS spark_streams
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE spark_streams;

CREATE TABLE IF NOT EXISTS created_users (
    id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    post_code TEXT,
    email TEXT,
    username TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);

DESCRIBE TABLE created_users;

select * from spark_streams.created_users;


try this  
set PYSPARK_PYTHON=C:\Users\user\AppData\Local\Programs\Python\Python312\python.exe

spark-submit --master spark://172.18.0.3:7077 spark_stream.py

if didint work do this 



docker exec -it projet-end-to-end1-spark-master-1 /bin/bash

pip install cassandra-driver

exit

docker cp C:\Users\user\Documents\Projet-end-to-end1\spark_stream.py projet-end-to-end1-spark-master-1:/opt/bitnami/spark/spark_stream.py

docker exec -it projet-end-to-end1-spark-master-1 spark-submit --master spark://172.18.0.3:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 /opt/bitnami/spark/spark_stream.py


docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042

select * from spark_streams.created_users;

cqlsh> PAGING ON;
cqlsh> PAGING 50;

cqlsh> SELECT id, first_name, last_name, email FROM spark_streams.created_users;

cqlsh>COPY spark_streams.created_users TO 'created_users.csv' WITH HEADER = TRUE;

docker cp cassandra:/created_users.csv C:\Users\user\Documents\Projet-end-to-end1\created_users.csv



last version 
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fonction pour créer un keyspace Cassandra
def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace 'spark_streams' created successfully!")
    except Exception as e:
        logging.error(f"Error creating keyspace: {e}")

# Fonction pour créer une table dans Cassandra
def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT);
        """)
        logging.info("Table 'created_users' created successfully in keyspace 'spark_streams'!")
    except Exception as e:
        logging.error(f"Error creating table: {e}")

# Fonction pour créer une connexion Spark
def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        # Configuration pour ignorer les logs non nécessaires
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


# Fonction pour connecter Kafka à Spark
def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created: {e}")
        return None

# Fonction pour créer une connexion Cassandra
def create_cassandra_connection():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['cassandra'], auth_provider=auth_provider)
        session = cluster.connect()
        logging.info("Connected to Cassandra successfully!")
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection: {e}")
        return None

# Fonction pour définir la structure des données et extraire les informations de Kafka
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    return sel

# Point d'entrée principal
if __name__ == "__main__":
    
    # Création de la connexion Spark
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        
        # Connexion à Kafka via Spark
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is not None:
            # Création de la connexion Cassandra
            session = create_cassandra_connection()

            if session is not None:
                # Création du keyspace et de la table dans Cassandra
                create_keyspace(session)
                session.set_keyspace('spark_streams')  # Connexion au keyspace
                create_table(session)

                logging.info("Streaming is being started...")

                # Définition du flux de données en temps réel vers Cassandra
                selection_df = create_selection_df_from_kafka(spark_df)
                streaming_query = (selection_df.writeStream
                                   .format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')  # emplacement pour le checkpoint
                                   .option('keyspace', 'spark_streams')  # keyspace dans Cassandra
                                   .option('table', 'created_users')  # table dans le keyspace
                                   .start())

                streaming_query.awaitTermination()

