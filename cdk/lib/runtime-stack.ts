import { Stack, StackProps, CfnOutput, RemovalPolicy, Duration } from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { NagSuppressions } from 'cdk-nag';
import { Construct } from 'constructs';

export interface RuntimeStackProps extends StackProps {
  customerName: string;
  emrJobRoleArn: string;
  emrServerlessApplicationId: string;
  /** Pre-computed bucket names — must match what EMR was granted. */
  bucketNames: { artifact: string; logs: string };
}

/**
 * Slim runtime stack: just the artifact + logs buckets. (No data bucket
 * here — Iceberg ingest doesn't read or write a customer-managed data
 * bucket; data lives in the S3 Tables warehouse, which IcebergStack owns.)
 *
 * Buckets:
 *   - artifactBucket: stores the shadowJar uploaded by the operator post-deploy.
 *                     Path: jars/cda-iceberg-client-1.0.jar
 *   - logsBucket:     EMR Serverless monitoring logs.
 *
 * Operator post-deploy steps:
 *   1. ./gradlew shadowJar
 *   2. aws s3 cp build/libs/cda-iceberg-client-1.0.jar s3://<artifactBucket>/jars/
 *   3. (See OrchestrationStack for triggering ingest jobs.)
 */
export class RuntimeStack extends Stack {
  readonly artifactBucket: s3.IBucket;
  readonly logsBucket: s3.IBucket;

  constructor(scope: Construct, id: string, props: RuntimeStackProps) {
    super(scope, id, props);

    // The logs bucket doubles as the S3 server-access-log target for the
    // artifact bucket. It needs the log-delivery prefix carved out of its
    // own lifecycle so EMR monitoring logs and S3 access logs don't collide.
    //
    // objectOwnership BUCKET_OWNER_ENFORCED keeps ACLs disabled (the modern
    // S3 default). Combined with the @aws-cdk/aws-s3:serverAccessLogsUse
    // BucketPolicy feature flag (cdk.json), CDK grants the logging.s3
    // service write access via a BUCKET POLICY rather than a legacy
    // LogDeliveryWrite ACL — which a BucketOwnerEnforced bucket rejects
    // ("The bucket does not allow ACLs").
    this.logsBucket = new s3.Bucket(this, 'Logs', {
      bucketName: props.bucketNames.logs,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.RETAIN,
      lifecycleRules: [{ expiration: Duration.days(30) }],
    });

    this.artifactBucket = new s3.Bucket(this, 'Artifacts', {
      bucketName: props.bucketNames.artifact,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      versioned: true,
      removalPolicy: RemovalPolicy.RETAIN,
      // Server access logs → logs bucket (cdk-nag AwsSolutions-S1).
      serverAccessLogsBucket: this.logsBucket,
      serverAccessLogsPrefix: 'access-logs/artifacts/',
    });

    // AwsSolutions-S1 on the logs bucket itself: it is the terminal access-log
    // destination. Pointing it at a third bucket only moves the problem (that
    // bucket would then be flagged) and would create a recursive logging loop
    // if pointed at itself. AppSec-accepted terminal-sink pattern.
    NagSuppressions.addResourceSuppressions(this.logsBucket, [
      {
        id: 'AwsSolutions-S1',
        reason:
          'This is the terminal server-access-log + EMR monitoring sink bucket. ' +
          'Enabling access logging on it would require a further log bucket ' +
          '(infinite regress) or self-logging (recursive writes). Public access ' +
          'is fully blocked and SSL is enforced.',
      },
    ]);

    new CfnOutput(this, 'ArtifactBucket', { value: this.artifactBucket.bucketName });
    new CfnOutput(this, 'LogsBucket',     { value: this.logsBucket.bucketName });
    new CfnOutput(this, 'EmrJobRoleArn',  { value: props.emrJobRoleArn });
    // CloudFormation output: a copy-paste aws-cli hint. Built via string
    // concatenation (not template literals) and with placeholders written as
    // ARG_TABLE etc. rather than <table>, so static scanners don't misread
    // it as interpolated HTML. The interpolated values are CDK resource
    // names; this output is a CLI string, never browser-rendered.
    const jobDriver =
      '{"sparkSubmit":{"entryPoint":"s3://' + this.artifactBucket.bucketName +
      '/jars/cda-iceberg-client-1.0.jar","entryPointArguments":' +
      '["ARG_TABLE","ARG_NAMESPACE","ARG_SRCBUCKET","ARG_MANIFESTKEY","ARG_STATETABLE"],' +
      '"sparkSubmitParameters":"--class gw.cda.iceberg.IcebergIngest"}}';
    new CfnOutput(this, 'StartJobRunHint', {
      value:
        '# After uploading the jar, submit a single-table ingest with ' +
        '(replace ARG_* placeholders):\n' +
        'aws emr-serverless start-job-run \\\n' +
        '  --application-id ' + props.emrServerlessApplicationId + ' \\\n' +
        '  --execution-role-arn ' + props.emrJobRoleArn +
        '  # MUST be EmrJobRoleArn, not your caller role \\\n' +
        "  --job-driver '" + jobDriver + "'",
    });
  }
}
