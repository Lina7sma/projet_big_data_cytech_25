from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


SBT_ENV = (
    "export HOME=/tmp && "
    "export COURSIER_CACHE=/tmp/.coursier && "
    "export SBT_OPTS='-Xms256m -Xmx1g -XX:MaxMetaspaceSize=512m "
    "-Dsbt.boot.directory=/tmp/.sbt/boot "
    "-Dsbt.ivy.home=/tmp/.ivy2 "
    "-Dsbt.global.base=/tmp/.sbt "
    "-Dsbt.coursier.home=/tmp/.coursier' && "
)

JAVA_OPENS = (
    "export JAVA_TOOL_OPTIONS='"
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED"
    "' && "
)

# Chemin du projet dans le container airflow (monté via docker-compose)
BASE_DIR = "/opt/project"

# Commandes pour chaque exercice
CMD_EX01 = "sbt clean compile run"
CMD_EX02 = "sbt clean compile run"
CMD_EX03 = (
    "export PGPASSWORD=postgres && "
    "psql -h postgres -U postgres -d postgres -f creation.sql && "
    "psql -h postgres -U postgres -d postgres -f insertion.sql"
)

# Streamlit en background (sinon ça bloque la DAG)
CMD_EX04 = "nohup python3 -m streamlit run app.py --server.headless true --server.port 8501 --server.address 0.0.0.0 > streamlit.log 2>&1 &"
CMD_EX05 = "./app.py"
CMD_EX06 = "echo 'Airflow orchestration done'"

with DAG(
        dag_id="bigdata_all_exercices",
        start_date=datetime(2025, 1, 1),
        schedule=None,  # lancement manuel
        catchup=False,
        tags=["bigdata"],
) as dag:

    ex01 = BashOperator(
        task_id="ex01_data_retrieval",
        bash_command=(
            f"{SBT_ENV}"
            f"{JAVA_OPENS}"
            f"cd {BASE_DIR}/ex01_data_retrieval && "
            f"sbt clean compile run"),
    )

    ex03 = BashOperator(
        task_id="ex03_sql_table_creation",
        bash_command=f"cd {BASE_DIR}/ex03_sql_table_creation && {CMD_EX03}",
    )

    ex02 = BashOperator(
        task_id="ex02_data_ingestion",
        bash_command=(
            f"{SBT_ENV}"
            f"{JAVA_OPENS}"
            f"cd {BASE_DIR}/ex02_data_ingestion && "
            f"sbt clean compile run"
        ),
    )

    ex04 = BashOperator(
        task_id='ex04_dashboard',
        bash_command="""
            for i in $(seq 1 30); do
              if curl -sSf http://airflow-webserver:8501/ >/dev/null 2>&1; then
                echo "Streamlit reachable"
                exit 0
              fi
              echo "Waiting for Streamlit ($i/30)..."
              sleep 2
            done
            echo "Streamlit not reachable after retries" >&2
            exit 1
            """,
        dag=dag
    )

    ex05 = BashOperator(
        task_id="ex05_ml_training",
        bash_command=(
            f"cd {BASE_DIR}/ex05_ml_prediction_service && "
            "python3 train.py"
        ),
    )


    ex06 = BashOperator(
        task_id="ex06_airflow",
        bash_command=f"cd {BASE_DIR}/ex06_airflow && {CMD_EX06}",
    )

    # Ordre
    ex01 >> ex03 >> ex02 >> ex04 >> ex05 >> ex06