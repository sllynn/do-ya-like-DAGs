## Databricks and Airflow

### Best practice guides

The advice contained in this document and accompanying example python script are very basic.

See the following articles for more detailed help on integrating Databricks and Airflow:

- https://docs.databricks.com/dev-tools/data-pipelines.html#apache-airflow
- https://databricks.com/blog/2017/07/19/integrating-apache-airflow-with-databricks.html 

For questions regarding Airflow itself, the docs site is very comprehensive and has some good worked examples:
- https://airflow.apache.org/index.html

### Basics

#### Installation and configuration

- GCC must be installed first

- If you're using containers or EC2 then port 8080 needs to be open to access the scheduler user interface

- Install Miniconda (if not already available)

- Use conda to create and activate a new airflow environment:
  - `conda create -n airflow python=3.7`
  - `conda activate airflow`
  
- Install airflow (using pip as it is not available on conda-forge): `pip install airflow`

- Run the database set-up routine: `airflow initdb`

- Start the scheduler: `airflow scheduler &`

- Start the web server: `airflow webserver &`

- Navigate to the web UI at `http://localhostname_or_ec2publicip:8080`

- Create a PAT token in your Databricks workspace
  - Click on the little person in the top right of the screen and select 'User'.
  - Navigate to the 'Tokens' tab.
  - Follow the instructions to create a new token.
  
- Configure the connection to your Databricks workspace:
  - From 'Admin', select 'Connections'
  - Click on the pencil icon next to the 'databricks_default' connection
  - Enter the URL to your workspace in the 'host' field, e.g. `stuart.cloud.databricks.com`
  - In the 'extra' field, enter your PAT token using the `{"token": "<your personal access token>"}` JSON format
  - The airflow CLI will also permit you to add or edit connections using the `airflow connections` interface, for example: 
  
  ```bash
  airflow connections --add \
  	--conn_id my_databricks_workspace \
  	--conn_type databricks \
  	--conn_host stuart.cloud.databricks.com \
  	--conn_extra '{"token": "<your personal access token>"}'
  ```
  
  - If you do create a new connection for your workspace, you must remember to specify this using the `databricks_conn_id` parameter in calls to `DatabricksSubmitRunOperator` and `DatabricksRunNowOperator`.



#### Creating a 'DAG': Directed, Acyclic (computational) Graph

- Expressed in python, but the script is not designed for execution - it just represents the configuration
- Can mix and match Databricks jobs with other airflow operators (bash scripts, python functions etc.)
- Example of a DAG running three existing Databricks jobs (a notebook job, a python script job and a spark-submit job) with parameters, in a specific order:

```python
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


```

- Store your scripts in `~/airflow/dags/`
- To see the full list of DAGs available and see if the `example_dependent_databricks_jobs` is now present, run `airflow list_dags`
- You can enable or trigger your DAG in the scheduler using the web UI or trigger it manually using: `airflow trigger_dag example_sequential_tasks`





