import { Stack, StackProps, CfnOutput, Tags } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as emrserverless from 'aws-cdk-lib/aws-emrserverless';
import { NagSuppressions } from 'cdk-nag';
import { Construct } from 'constructs';

export interface EmrStackProps extends StackProps {
  customerName: string;
  vpc: ec2.IVpc;
  computeSubnets: ec2.SubnetSelection;
  serverlessReleaseLabel: string;
  /** ARN of the CDA source bucket (cross-account read). Empty string is
   * accepted only for first-deploy stub mode where the operator hasn't
   * pointed CDA at a bucket yet — orchestration won't run until set. */
  cdaSourceBucketArn: string;
  /** ARN of the S3 Tables bucket created by IcebergStack. The job role gets
   * read+write on this resource. */
  icebergTableBucketArn: string;
  /**
   * Pre-computed bucket names from the runtime stack (artifact + logs).
   * Granted by literal ARN to avoid a stack-cycle with RuntimeStack.
   */
  bucketNames: { artifact: string; logs: string };

  /** EMR Serverless application maximum capacity. Total ceiling across
   * all concurrent jobs. Defaults sized for a tri-suite mid-size carrier;
   * shrink for small customers (smaller AWS quota footprint), grow for
   * large carriers running many concurrent jobs. */
  emrMaxVcpu: string;
  emrMaxMemory: string;
  emrMaxDisk: string;
  /** Minutes the application stays warm after the last job finishes.
   * Higher = fewer cold-start hits at high event rates (lifecycle events),
   * but more idle billing. Lower = cheaper for sparse / hourly workloads. */
  emrIdleTimeoutMinutes: number;
  /** Number of pre-warmed Driver workers held in initialCapacity. Each
   * concurrent EMR job claims one driver from the warm pool (or cold-starts
   * if exhausted). Defaults to 4 — covers the first wave at the default
   * mapStateConcurrency=8 with 2 cold starts (vs. 7 cold starts at the
   * old default of 1). Bump to mapStateConcurrency for zero-cold-start
   * bulk loads at the cost of extra idle billing. */
  emrInitialDriverCount: number;
  /** Number of pre-warmed Executor workers held in initialCapacity. Roughly
   * 2-3× emrInitialDriverCount is a reasonable starting point. Default 8
   * matches the pattern of 2 executors per concurrent job at concurrency=4
   * warm. Higher = faster bulk-load wave 1, more idle cost. */
  emrInitialExecutorCount: number;
}

/**
 * EMR Serverless application + IAM job role for the Iceberg ingest path.
 *
 * Differs from OSR's EMR stack on two axes:
 *
 * 1. Single flavor (Serverless only). EMR-on-EC2 isn't supported in this
 *    repo — Iceberg ingest fans out per-table via Step Functions Map state,
 *    which works much better against a serverless application than a
 *    long-lived EC2 cluster.
 *
 * 2. No RDS secret access in the role. The Iceberg path doesn't write to
 *    RDS directly. (RDS hydration is out of scope for this sample; if you
 *    add such a job, give it its own role with secret-read scoped to itself
 *    rather than widening this one.)
 *
 * Job role permissions:
 *   - read on artifact bucket (jar)
 *   - read+write on logs bucket (EMR monitoring)
 *   - read on CDA source bucket (parquet)
 *   - read+write on the S3 Tables warehouse (Iceberg metadata + data)
 *   - s3tables:* on the table bucket (managed Iceberg API)
 *   - glue:GetDatabase / GetTable for Iceberg's catalog calls
 */
export class EmrStack extends Stack {
  readonly jobRole: iam.Role;
  readonly serverlessApplicationId: string;

  constructor(scope: Construct, id: string, props: EmrStackProps) {
    super(scope, id, props);

    const computeSg = new ec2.SecurityGroup(this, 'ComputeSg', {
      vpc: props.vpc,
      description: `${props.customerName} EMR compute SG`,
      allowAllOutbound: true,
    });

    this.jobRole = new iam.Role(this, 'EmrJobRole', {
      assumedBy: new iam.ServicePrincipal('emr-serverless.amazonaws.com'),
      description: `${props.customerName} CDA Iceberg ingest role`,
    });

    const bucketArn = (n: string) => `arn:${this.partition}:s3:::${n}`;

    // Artifact bucket: read-only (jar).
    this.jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'ReadArtifactBucket',
        actions: ['s3:GetObject', 's3:ListBucket'],
        resources: [
          bucketArn(props.bucketNames.artifact),
          `${bucketArn(props.bucketNames.artifact)}/*`,
        ],
      }),
    );

    // Logs bucket: write-only (EMR monitoring writes here).
    this.jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'WriteLogsBucket',
        actions: ['s3:PutObject'],
        resources: [`${bucketArn(props.bucketNames.logs)}/*`],
      }),
    );

    // CDA source bucket. Cross-account: the bucket-side policy must also
    // allow this role. Operator's responsibility — the README documents it.
    if (props.cdaSourceBucketArn) {
      this.jobRole.addToPolicy(
        new iam.PolicyStatement({
          sid: 'ReadCdaSource',
          actions: ['s3:GetObject', 's3:ListBucket', 's3:GetBucketLocation'],
          resources: [
            props.cdaSourceBucketArn,
            `${props.cdaSourceBucketArn}/*`,
          ],
        }),
      );
    }

    // S3 Tables bucket: full read/write via the s3tables service API for
    // Iceberg metadata operations, plus underlying data S3 access.
    this.jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'IcebergTableOps',
        actions: [
          's3tables:GetTableBucket',
          's3tables:ListNamespaces',
          's3tables:GetNamespace',
          's3tables:ListTables',
          's3tables:CreateTable',
          's3tables:GetTable',
          's3tables:GetTableMetadataLocation',
          's3tables:UpdateTableMetadataLocation',
          's3tables:GetTableData',
          's3tables:PutTableData',
          's3tables:DeleteTableData',
          's3tables:GetTableMaintenanceConfiguration',
        ],
        resources: [
          props.icebergTableBucketArn,
          `${props.icebergTableBucketArn}/*`,
        ],
      }),
    );

    // CloudWatch Logs delivery for EMR Serverless monitoring. The job's
    // log-pusher CALLS logs:DescribeLogGroups before writing (to validate the
    // destination); that action is not resource-scopable (AWS evaluates it
    // against log-group::log-stream:), so it must be granted on "*". The
    // write actions stay scoped to the /aws/emr-serverless/* prefix.
    this.jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'CloudWatchLogsDescribe',
        actions: ['logs:DescribeLogGroups'],
        resources: ['*'],
      }),
    );
    this.jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'CloudWatchLogs',
        actions: [
          'logs:CreateLogGroup',
          'logs:CreateLogStream',
          'logs:PutLogEvents',
        ],
        resources: [
          `arn:${this.partition}:logs:${this.region}:${this.account}:log-group:/aws/emr-serverless/*`,
          `arn:${this.partition}:logs:${this.region}:${this.account}:log-group:/aws/emr-serverless/*:log-stream:*`,
        ],
      }),
    );

    Tags.of(this.jobRole).add('cda:customer', props.customerName);

    const subnetIds = props.vpc.selectSubnets(props.computeSubnets).subnetIds;
    const app = new emrserverless.CfnApplication(this, 'ServerlessApp', {
      name: `${props.customerName}-cda-iceberg`,
      // emr-spark-8.0-preview is the only release (late 2025) that ships
      // Spark 4.0.1, which the jar is built against.
      releaseLabel: props.serverlessReleaseLabel,
      type: 'SPARK',
      networkConfiguration: {
        subnetIds,
        securityGroupIds: [computeSg.securityGroupId],
      },
      autoStartConfiguration: { enabled: true },
      autoStopConfiguration: { enabled: true, idleTimeoutMinutes: props.emrIdleTimeoutMinutes },
      // Initial capacity is the warm pool. Each per-table EMR job claims
      // a driver and N executors from this pool when it starts; if the
      // pool is exhausted, the job cold-starts (~30 s overhead). Sized
      // by emrInitialDriverCount / emrInitialExecutorCount context flags
      // — bump them to match mapStateConcurrency for zero-cold-start
      // bulk loads at the cost of extra idle billing.
      initialCapacity: [
        {
          key: 'Driver',
          value: {
            workerCount: props.emrInitialDriverCount,
            workerConfiguration: { cpu: '4 vCPU', memory: '12 GB' },
          },
        },
        {
          key: 'Executor',
          value: {
            workerCount: props.emrInitialExecutorCount,
            workerConfiguration: { cpu: '4 vCPU', memory: '16 GB' },
          },
        },
      ],
      maximumCapacity: { cpu: props.emrMaxVcpu, memory: props.emrMaxMemory, disk: props.emrMaxDisk },
    });

    this.serverlessApplicationId = app.attrApplicationId;

    // ── cdk-nag suppressions (evidence for AppSec) ──────────────────────
    // All wildcards below are object-key / log-stream wildcards on
    // resources we DO scope by ARN — not service-level "*" grants. S3 and
    // CloudWatch Logs require the trailing /* (you cannot enumerate object
    // keys or log streams at deploy time). This is the documented, expected
    // shape for least-privilege object access; the bucket/log-group ARNs
    // themselves are pinned.
    NagSuppressions.addResourceSuppressions(
      this.jobRole,
      [
        {
          id: 'AwsSolutions-IAM5',
          reason:
            'Object-level wildcard on explicitly-pinned bucket ARNs (artifact, ' +
            'logs, CDA source, S3 Tables warehouse). S3 GetObject/PutObject ' +
            'require <bucket>/* — object keys (parquet partitions, jar versions, ' +
            'Iceberg metadata) are not enumerable at synth time. The bucket ARNs ' +
            'are fixed; no service-level s3:* or Resource:* is granted.',
          // Customer-portable but NOT over-broad: this role only ever holds
          // S3 and CloudWatch-Logs grants, so we match object-key wildcards
          // on (a) any literal S3 ARN — bucket names embed the concrete
          // account id, so a fixed string can't be customer-portable — and
          // (b) the S3 Tables warehouse bucket, which cdk-nag renders as a
          // cross-stack token. The alternation is deliberately limited to
          // these two shapes: an object-key wildcard on any OTHER resource
          // type (e.g. a future DynamoDB/SNS token) will NOT match and will
          // surface as a fresh finding for review.
          appliesTo: [
            { regex: '/^Resource::(arn:aws:s3:::.+|<CdaTableBucket.*>)\\/\\*$/g' },
          ],
        },
        {
          id: 'AwsSolutions-IAM5',
          reason:
            'CloudWatch Logs delivery for EMR Serverless monitoring. EMR creates ' +
            'one log group per application/job under /aws/emr-serverless/*; the ' +
            'group names are not known at deploy time. Scoped to this account, ' +
            'this region, and the /aws/emr-serverless/ prefix.',
          appliesTo: [
            { regex: '/^Resource::arn:aws:logs:.*:log-group:\\/aws\\/emr-serverless\\/\\*.*$/g' },
          ],
        },
        {
          id: 'AwsSolutions-IAM5',
          reason:
            'logs:DescribeLogGroups is required by the EMR Serverless log-pusher ' +
            'to validate the CloudWatch log destination before writing. AWS ' +
            'evaluates this list-type action against log-group::log-stream: (no ' +
            'resource-level ARN), so it must be granted on Resource:*. The actual ' +
            'WRITE actions (CreateLogGroup/Stream, PutLogEvents) remain scoped to ' +
            'the /aws/emr-serverless/ prefix.',
          appliesTo: ['Resource::*'],
        },
      ],
      true, // applyToChildren — DefaultPolicy is a child of the role
    );

    new CfnOutput(this, 'ServerlessApplicationId', { value: this.serverlessApplicationId });
    new CfnOutput(this, 'EmrJobRoleArn',           { value: this.jobRole.roleArn });
    new CfnOutput(this, 'ComputeSecurityGroupId',  { value: computeSg.securityGroupId });
  }
}
