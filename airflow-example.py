from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator, DatabricksSubmitRunOperator

# In this example, we will create an airflow 'DAG' that will execute a notebook and spark JAR task
# independently and, once the first two jobs have successfully completed, a python script.

# The starting point is the DAG, itself. It has some basic parameters around scheduling and alerting which
# should be configured on instantiation.
dag = DAG(
    dag_id='example_dependent_databricks_jobs',
    start_date=datetime.today(),
    schedule_interval=timedelta(days=1),
    default_args=dict(
        email=['stuart@databricks.com'],
        email_on_failure=True,
        email_on_success=True,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5)
    )
)

# Our first task will execute a notebook on an 'ephemeral' cluster (i.e. the cluster is created purely to
# execute the job and then torn down once the job is complete).
# Let's assume our notebook also has a text widget, the value of which controls where output will be stored.

cluster_spec = {
    'spark_version': '6.0.x-scala2.11',
    'node_type_id': 'i3.xlarge',
    'aws_attributes': {
        'availability': 'ON_DEMAND'
    },
    'num_workers': 4
}

notebook_task_params = {
    'new_cluster': cluster_spec,
    'notebook_task': {
        'notebook_path': '/Users/stuart@databricks.com/PrepareData',
        'base_parameters': {
            'output_path': '/mnt/path/to/output'
        }
    },
}

# The above block of key-value parameters are equivalent to the 'new cluster' and 'notebook task' objects
# supplied to the Databricks Runs Submit API.
# More info here: https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-submit
# and here: https://docs.databricks.com/dev-tools/api/latest/jobs.html#newcluster
# and here: https://docs.databricks.com/dev-tools/api/latest/jobs.html#notebooktask

# We'll feed all of our parameters to the DatabricksSubmitRunOperator via its `JSON` parameter.
notebook_task = DatabricksSubmitRunOperator(
    task_id='notebook_task',
    dag=dag,
    json=notebook_task_params)

# Our second task, which is independent of the first, executes a spark JAR (i.e. compiled Scala code).
# Rather than construct our task in one block of key-value parameters, we'll use the named parameters
# of DatabricksSubmitRunOperator to initialize the operator.
# Again, this will create a new cluster for the duration of the task.
spark_jar_task = DatabricksSubmitRunOperator(
    task_id='spark_jar_task',
    dag=dag,
    new_cluster=cluster_spec,
    spark_jar_task={
        'main_class_name': 'com.example.ProcessData'
    },
    libraries=[
        {
            'jar': 'dbfs:/lib/etl-0.1.jar'
        }
    ]
)
# The 'libraries' argument allows you to attach libraries to the cluster that will be instantiated
# to execute the task.

# Finally, we have a python script that we wish to execute on an existing cluster, on successful completion
# of both of the first two tasks.

# In this case, the job is already defined in the Databricks workspace and `job_id` is used to identify it.
# Arguments can be passed to the job using `notebook_params`, `python_params` or `spark_submit_params`
# as appropriate.
# The association with the existing cluster would be part of that existing job's definition and configurable through
# the Jobs UI in the Databricks workspace.
spark_python_task = DatabricksRunNowOperator(
    task_id='spark_python_task',
    dag=dag,
    job_id=1337,
    python_params=['country', 'DK']
)

# Define the order in which these jobs must run
notebook_task.set_downstream(spark_python_task)
spark_jar_task.set_downstream(spark_python_task)
