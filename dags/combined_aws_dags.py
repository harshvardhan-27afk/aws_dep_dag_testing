"""
combined_aws_dags.py : one DAG that exercises the AWS operators in Maestro-PI
that work on a free AWS account: 12 operators and 3 sensors.

Deploy aws/aws_setup.yaml (CloudFormation) first and attach its
MaestroTestPolicyArn output to the IAM user/role of your Maestro-PI AWS
connection. Then edit the constants below, push this file to your DAGs git repo
and trigger the DAG manually (schedule=None).

NOT covered (they need paid services): Glue, EMR, Redshift and their sensors.
AthenaSensor is also left out: it needs the query id that AthenaOperator
produces, and Maestro operator params cannot read another task's XCom.

Task graph:

  S3 chain
    t01_s3_create [S3CreateObject] -> t02_s3_key_sensor [S3KeySensor]
      -> t03_s3_copy [S3CopyObject] -+-> t04_s3_list [S3List]
                                     +-> t05_s3_read [S3ReadObject]
      -> (both) t06_s3_delete [S3DeleteObjects]

  Messaging
    t07_sns_publish [SnsPublish]                    (independent)
    t08_sqs_publish [SqsPublish] -> t09_sqs_sensor [SqsSensor]

  Lambda + DynamoDB
    t10_lambda_seed [LambdaInvokeFunction] -> t11_dynamodb_to_s3 [DynamoDBToS3]

  Step Functions
    t12_sfn_start [StepFunctionStartExecution]
      -> t13_sfn_sensor [StepFunctionExecutionSensor]
      -> t14_sfn_output [StepFunctionGetExecutionOutput]

  Athena
    t15_athena [Athena]                              (independent)

Maestro passes values between tasks only through Python callables, so the
Step Functions chain uses a deterministic execution name built from the run
timestamp ({{ .TSNodash }}) and builds the execution ARN from it.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    S3CreateObjectOperator,
    S3KeySensor,
    S3CopyObjectOperator,
    S3ListOperator,
    S3ReadObjectOperator,
    S3DeleteObjectsOperator,
    SnsPublishOperator,
    SqsPublishOperator,
    SqsSensor,
    LambdaInvokeFunctionOperator,
    DynamoDBToS3Operator,
    StepFunctionStartExecutionOperator,
    StepFunctionExecutionSensor,
    StepFunctionGetExecutionOutputOperator,
    AthenaOperator,
)

# --- Edit these to match your account / Maestro-PI connection ----------------
AWS_CONN_ID = "aws_default"        # Maestro-PI AWS connection name (Connections page)
AWS_REGION = "ap-southeast-2"           # region you deployed the stack in
AWS_ACCOUNT_ID = "756921148520"    # your 12-digit AWS account id
PREFIX = "piflow-aws"              # ResourcePrefix parameter of the stack
# ------------------------------------------------------------------------------

# Names the stack creates (derived from PREFIX; see aws_setup.yaml).
BUCKET = f"{PREFIX}-{AWS_ACCOUNT_ID}-{AWS_REGION}"
TOPIC_ARN = f"arn:aws:sns:{AWS_REGION}:{AWS_ACCOUNT_ID}:{PREFIX}-topic"
QUEUE_URL = f"https://sqs.{AWS_REGION}.amazonaws.com/{AWS_ACCOUNT_ID}/{PREFIX}-queue"
TABLE = f"{PREFIX}-items"
FUNCTION = f"{PREFIX}-fn"
STATE_MACHINE_ARN = f"arn:aws:states:{AWS_REGION}:{AWS_ACCOUNT_ID}:stateMachine:{PREFIX}-machine"

# Execution name rendered from the run timestamp, identical in every task of one
# run. Step Functions names allow letters, digits, - and _ only.
SFN_EXEC_NAME = "maestro-{{ .TSNodash }}"
SFN_EXEC_ARN = (
    f"arn:aws:states:{AWS_REGION}:{AWS_ACCOUNT_ID}:execution:{PREFIX}-machine:"
    + SFN_EXEC_NAME
)


with DAG(
    dag_id="combined_aws_dags",
    description="Free-account AWS operators: S3, SNS, SQS, Lambda, DynamoDB, Step Functions, Athena",
    tags=["aws", "operator-test"],
    schedule=None,                 # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        # Every AWS task resolves its connection and region from here.
        "aws_conn_id": AWS_CONN_ID,
        "region_name": AWS_REGION,
    },
) as dag:

    # ---------------- S3 ----------------
    # 1) Writes a small text object. replace=True so re-runs overwrite it.
    t01_s3_create = S3CreateObjectOperator(
        task_id="t01_s3_create",
        s3_bucket=BUCKET,
        s3_key="input/hello.txt",
        data="hello from Maestro-PI",
        replace=True,
    )

    # 2) Waits until the object exists (it already does, so it passes at once).
    t02_s3_key_sensor = S3KeySensor(
        task_id="t02_s3_key_sensor",
        bucket_name=BUCKET,
        bucket_key="input/hello.txt",
        poke_interval=5,
        timeout=120,
    )

    # 3) Copies input/hello.txt to copy/hello_copy.txt (overwrites on re-run).
    t03_s3_copy = S3CopyObjectOperator(
        task_id="t03_s3_copy",
        source_bucket_name=BUCKET,
        source_bucket_key="input/hello.txt",
        dest_bucket_name=BUCKET,
        dest_bucket_key="copy/hello_copy.txt",
    )

    # 4) Lists keys under the bucket; the key list goes to XCom.
    t04_s3_list = S3ListOperator(
        task_id="t04_s3_list",
        bucket=BUCKET,
        prefix="",
    )

    # 5) Reads the copied object back; its text goes to XCom.
    t05_s3_read = S3ReadObjectOperator(
        task_id="t05_s3_read",
        s3_bucket=BUCKET,
        s3_key="copy/hello_copy.txt",
    )

    # 6) Deletes the copy (input/hello.txt stays).
    t06_s3_delete = S3DeleteObjectsOperator(
        task_id="t06_s3_delete",
        bucket=BUCKET,
        keys=["copy/hello_copy.txt"],
    )

    t01_s3_create >> t02_s3_key_sensor >> t03_s3_copy >> [t04_s3_list, t05_s3_read] >> t06_s3_delete

    # ---------------- SNS / SQS ----------------
    # 7) Publishes to the topic. The stack subscribes an SQS "sink" queue, so the
    #    message appears there.
    t07_sns_publish = SnsPublishOperator(
        task_id="t07_sns_publish",
        target_arn=TOPIC_ARN,
        message="hello from Maestro-PI via SNS",
        subject="maestro-pi-test",
    )

    # 8) Sends a message to the queue.
    t08_sqs_publish = SqsPublishOperator(
        task_id="t08_sqs_publish",
        sqs_queue=QUEUE_URL,
        message_content="hello from Maestro-PI via SQS",
    )

    # 9) Receives the message t08 sent and deletes it from the queue.
    t09_sqs_sensor = SqsSensor(
        task_id="t09_sqs_sensor",
        sqs_queue=QUEUE_URL,
        max_messages=5,
        wait_time_seconds=5,
        poke_interval=10,
        timeout=180,
        delete_message_on_reception=True,
    )

    t08_sqs_publish >> t09_sqs_sensor

    # ---------------- Lambda -> DynamoDB -> S3 ----------------
    # 10) Invokes the stack's Lambda with {"action": "seed"}; it writes 10 items
    #     into the DynamoDB table. The response payload goes to XCom.
    t10_lambda_seed = LambdaInvokeFunctionOperator(
        task_id="t10_lambda_seed",
        function_name=FUNCTION,
        payload='{"action": "seed"}',
    )

    # 11) Scans the table and writes the items as JSON lines to
    #     s3://<bucket>/dynamodb-export/.
    t11_dynamodb_to_s3 = DynamoDBToS3Operator(
        task_id="t11_dynamodb_to_s3",
        dynamodb_table_name=TABLE,
        s3_bucket_name=BUCKET,
        s3_key_prefix="dynamodb-export/",
    )

    t10_lambda_seed >> t11_dynamodb_to_s3

    # ---------------- Step Functions ----------------
    # 12) Starts an execution with a run-derived name (it does not wait).
    t12_sfn_start = StepFunctionStartExecutionOperator(
        task_id="t12_sfn_start",
        state_machine_arn=STATE_MACHINE_ARN,
        name=SFN_EXEC_NAME,
        state_machine_input={"source": "maestro-pi"},
    )

    # 13) Waits for that execution to reach SUCCEEDED.
    t13_sfn_sensor = StepFunctionExecutionSensor(
        task_id="t13_sfn_sensor",
        execution_arn=SFN_EXEC_ARN,
        poke_interval=5,
        timeout=180,
    )

    # 14) Fetches the finished execution's output JSON.
    t14_sfn_output = StepFunctionGetExecutionOutputOperator(
        task_id="t14_sfn_output",
        execution_arn=SFN_EXEC_ARN,
    )

    t12_sfn_start >> t13_sfn_sensor >> t14_sfn_output

    # ---------------- Athena ----------------
    # 15) Runs SELECT 1 (no table needed) and writes the result CSV under
    #     s3://<bucket>/athena-results/. Scans no data.
    t15_athena = AthenaOperator(
        task_id="t15_athena",
        query="SELECT 1 AS ok",
        output_location=f"s3://{BUCKET}/athena-results/",
        sleep_time=3,
    )
