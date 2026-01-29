# airflow-etl-pipeline
End-to-end ETL pipeline built using Apache Airflow. Automates data extraction, transformation, and loading using Python and Docker, with output validation through SQL table results.
This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline implemented using Apache Airflow.

The pipeline is orchestrated using Airflow DAGs and performs:
- Data extraction using Python
- Data transformation using Pandas
- Data loading into a database
- Output validation through SQL table queries

The entire workflow is containerized using Docker and Docker Compose, providing a reproducible and scalable data engineering setup.
