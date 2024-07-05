# Data Lineage - Getting Started
These are the instructions and getting started Python scripts for extracting and loading data lineage (preview) into Amazon DataZone. To view data lineage for data assets in the Amazon DataZone Portal, the data assets must be used as input to or output forom the processes that generate the data lienage.

* Blog article: [Amazon DataZone introduces OpenLineage-compatible data lineage visualization in preview](https://aws.amazon.com/blogs/big-data/amazon-datazone-introduces-openlineage-compatible-data-lineage-visualization-in-preview)

## General configuration
### Setting up your Python environment

1. Open a terminal window to the folder with this ReadMe file and the extract data lineage scripts.
1. Install Python
   ```  
   sudo yum -y install python3
   ```
1. Set the Python environment
   ```  
   python3 -m venv env
   . env/bin/activate
   ```
1. Install the Python packages required by the scripts: ```boto3```, ```openlineage_sql```, and ```redshift-connector```.
   ```
   pip3 install -r requirements.txt

### Configuring your AWS credentials
To configure your AWS credentials for the AWS SDK for Python (Boto3) follow these instructions:
* Configuration  
  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
* Credentials  
  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

**Optional IAM Permissions**  
These optional IAM API permissions are used by the scripts to display your IAM login account and identity when you are prompted to verify the script settings. If these permissions are not granted then your IAM login information will not be included in the verify settings prompt.

* AWS IAM ListAccountAliases (iam:ListAccountAliases)
* AWS Security Token Service GetCallerIdentity (sts:GetCallerIdentity)

## Capture AWS Glue crawler job data lineage
[extract_glue_crawler_lineage.py](extract_glue_crawler_lineage.py)  
This script extracts data lineage from the specified table's AWS Glue crawler job run updates then posts the data lineage to your Amazon DataZone domain.

**Required**  
These API permissions are required:

* AWS Glue GetTable (glue:GetTable)
* AWS Glue ListCrawls (glue:ListCrawls)
* Amazon DataZone PostLineageEvent (datazone:PostLineageEvent)

**Help command**
   ```
   python3 extract_glue_crawler_lineage.py --help
   ```
**Example command**
   ```
   python3 extract_glue_crawler_lineage.py \
     --region us-east-1 \
     --domain-identifier your_domain_id \
     --database-name your_database_name \
     --table_name your_table_name
   ```

## Capture AWS Glue Spark job data lineage
[extract_glue_spark_lineage.py](extract_glue_spark_lineage.py)  
In this example the AWS Glue Spark job data lineage is captured by the OpenLineage Spark plugin, is written out to the Spark console, then AWS Glue collects the Spark console messages and forwards them to the ```/aws-glue/jobs/error``` Amazon CloudWatch log group.

This script extracts the Spark data lineage from the ```/aws-glue/jobs/error``` log group then posts the data lineage to your Amazon DataZone domain. Data lineage is extracted from the log group messages starting from the specified ```--start-time```, or from the current time if no start time is provided. The log group is scanned for new data lineage every 15 seconds until the script is terminated by typing CTRL-C or the AWS credentials expire.

### Configuring the OpenLineage Spark plugin
To capture Spark data lineage you must add the OpenLineage Spark plugin to your AWS Glue Spark job and configure the plugin settings.

>[!IMPORTANT]
>The OpenLineage Spark plugin cannot capture data lineage from AWS Glue Spark jobs that use AWS Glue DynamicFrames. Use Spark SQL DataFrames instead.

1. You can download the OpenLineage Spark plugin jar version 1.9.1 for Scala 2.12 from this repository:
   * [OpenLineage Spark](https://mvnrepository.com/artifact/io.openlineage/openlineage-spark)
   * [Version 1.9.1](https://mvnrepository.com/artifact/io.openlineage/openlineage-spark_2.12/1.9.1)
   * [Download openlineage-spark_2.12-1.9.1.jar](https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.9.1/openlineage-spark_2.12-1.9.1.jar)
   
   The 1.9.1 version is compatible with AWS Glue version:
    * ```Glue 3.0 - Supports Spark 3.1, Scala 2, Python 3```
    
    If you are using a different AWS Glue version you will need to download the OpenLineage Spark plugin jar version that is compatible with your AWS Glue version.

1. Upload the ```openlineage-spark_2.12-1.9.1.jar``` file to an S3 bucket in the same account and region as your AWS Glue Spark job. Your AWS Glue job role must have permission to read the jar file from the S3 bucket.

### Configure the AWS Glue Spark job
Create a new or update an existing AWS Glue Spark job with the job settings below.

* **AWS Glue Spark job role**  
   The job role must have the permissions required to successfully execute your job and permission to read (download) the OpenLineage Spark plugin jar file from your S3 bucket.

* **Type**  
 ```Spark```

* **Glue version**  
  ```Glue 3.0 - Supports Spark 3.1, Scala 2, Python 3```  
  This AWS Glue version choice is compatible with the OpenLineage Spark plugin jar version 1.9.1 (see above). If you are using a different AWS Glue version you will need to download the OpenLineage Spark plugin jar version that is compatible with your AWS Glue version.

* **Advanced properties >** (expand)

  * **Dependent JARs path**  
    Add your S3 path to the OpenLineage Spark plugin jar.
 
    Example: ```s3://{your bucket name}/lib/openlineage-spark_2.12-1.9.1.jar```

  * **Job Parameters**  
    These parameters connect the OpenLineage Spark plugin and configure it to emit lineage to the Spark console. AWS Glue captures  Spark console output in the Amazon CloudWatch ```/aws/glue/error``` log group.
    
    >[!NOTE]
    >Additional ```--conf``` options after the first must be appended to the value for the first ```--conf``` option (key).

      * Key: ```--conf```  
        Value:
        ```spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener --conf spark.openlineage.transport.type=console --conf spark.openlineage.facets.custom_environment_variables=[AWS_DEFAULT_REGION;GLUE_VERSION;GLUE_COMMAND_CRITERIA;GLUE_PYTHON_VERSION;]```

      * Key: ```--user-jars-first```  
        Value: ```true```  
        This is recommended to avoid Java class load order issues.

### Running extract_glue_spark_lineage.py
**Required**  
Your IAM identity should be logged into the same account as the AWS Glue Spark job and have these API permissions:

* Amazon CloudWatch FilterLogEvents (logs:FilterLogEvents)
* Amazon DataZone PostLineageEvent (datazone:PostLineageEvent)

**Help command**
   ```
   python3 extract_glue_spark_lineage.py --help
   ```
**Example command**
   ```
   python3 extract_glue_spark_lineage.py \
     --region us-east-1 \
     --domain-identifier your_domain_id
   ```
**Example output**
   ```
  Posting data lineage for:
    Run ID:     40def005-88a8-4872-98be-8ef3c3dcacc2
    Event type: START
    Event time: 2024-05-13T16:22:19.457Z
    Job name:   nativespark_..._jr_...
   Succeeded.

   Searching for lineage data...
   No lineage data found.

   Pausing for 15 seconds (CTRL-C to quit)...
   ```

---
## Capture Amazon Redshift data lineage
[extract_redshift_lineage.py](extract_redshift_lineage.py)  
This script extracts table data lineage from Amazon Redshift SQL queries then posts the data lineage to your Amazon DataZone domain.

In this example we will use [Cloud 9](https://docs.aws.amazon.com/cloud9/latest/user-guide/welcome.html) so that you can configure private network access from the Cloud9 instance to your Amazon Redshift instance.

>[!IMPORTANT]
>Create your Cloud9 instance in the same VPC as your Amazon Redshift instance, then add your Cloud9 instance IP address to the inbound rules for your Amazon Redshift instance's security group.  
>See: [Private accessibility with default or custom security group configuration](https://docs.aws.amazon.com/redshift/latest/mgmt/rs-security-group-public-private.html#rs-security-group-private)

In your Cloud9 environment:
1. Select ```File``` and then ```Upload Local Files```
1. Select and upload the ```requirements.txt``` and ```extract_redshift_lineage.py``` files from this folder.
1. Open your Cloud9 environment terminal window then follow the instructions in [Setting up your Python environment](#setting-up-your-python-environment) (above).
1. Run the extract_redshift_lineage.py script (see example below). The script will prompt you to enter the user name and password for the connection to your Amazon Redshift database.

**Help command**
   ```
   python3 extract_redshift_lineage.py --help
   ```
**Example command**
   ```
   python3 extract_redshift_lineage.py \
     --region us-east-1 \
     --domain-identifier your_domain_id \
     --host-name your_redshift_instance_host_name \
     --database-name your_database_name
   ```

   The Amazon Redshift instance host name should be the host name only without the port or database name.  
   
   For example:
   * ```{cluster-name}.{id}.{aws-region}.redshift.amazonaws.com```
   * ```{workgroup-name}.{account-number}.{aws-region}.redshift-serverless.amazonaws.com```

## Capture Amazon MWAA data lineage
[extract_airflow_lineage.py](extract_airflow_lineage.py)  
In this example the Amazon MWAA data lineage is captured by the OpenLineage Airflow plugin, is written out to the console using the console transport, and then collected in the ```airflow-{Airflow_environemnt_name}-Task``` Airflow Task log group in Amazon CloudWatch.

This script extracts the data lineage from the ```airflow-{Airflow_environemnt_name}-Task``` log group then posts the data lineage to your Amazon DataZone domain. Data lineage is extracted from the log group messages starting from the specified ```--start-time```, or from the current time if no start time is provided. The log group is scanned for new data lineage every 15 seconds until the script is terminated by typing CTRL-C or the AWS credentials expire.

### Configuring the OpenLineage Airflow plugin
This example is for Apache Airflow version 2.6.3 in Amazon MWAA. For other Apache Airflow versions supported by OpenLineage, refer to [Supported Airflow versions](https://openlineage.io/docs/integrations/airflow/older/#:~:text=Airflow%202.7%2B%E2%80%8B,-airflow-providers-openlineage).
* In the Amazon MWAA console select or create an environment using Apache Airflow version 2.6.3.
* In your environment Requirements file (```requirements.txt```) that is stored in S3, include the OpenLineage Airflow plugin version 1.4.1.
   ```
   openlineage-airflow==1.4.1
   ```
* In the environment ```Configure advanced settings``` - ```Monitoring``` section:
   * Set ```CloudWatch Metrics``` to enabled
   * Set ```Airflow task logs``` to enabled and the log level to ```INFO```

### Running extract_airflow_lineage.py
**Required**  
Your IAM identity should be logged into the same account as your Amazon MWAA environment and have these API permissions:

* Amazon CloudWatch FilterLogEvents (logs:FilterLogEvents)
* Amazon DataZone PostLineageEvent (datazone:PostLineageEvent)

**Help command**
   ```
   python3 extract_airflow_lineage.py --help
   ```
**Example command**
   ```
   python3 extract_airflow_lineage.py \
     --region us-east-1 \
     --domain-identifier your_domain_id \
     --airflow-environment-name your_environment_name
   ```
**Example output**
   ```
   Posting data lineage for:
     Run ID:     9e6dba8b-c010-4930-b7a8-d09fb2f76ed6
     Event type: COMPLETE
     Event time: 2024-06-10T18:35:43.100203Z
     Job name:   ...job_name...
   Succeeded.

   Searching for lineage data...
   No lineage data found.

   Pausing for 15 seconds (CTRL-C to quit)...
   ```
