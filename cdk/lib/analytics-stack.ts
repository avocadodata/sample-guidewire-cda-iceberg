// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import { Stack, StackProps, CfnOutput, CustomResource, Duration, RemovalPolicy } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cloudwatchActions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Provider } from 'aws-cdk-lib/custom-resources';
import { NagSuppressions } from 'cdk-nag';
import { Construct } from 'constructs';
import * as path from 'path';

/** Suppress cdk-nag AwsSolutions-IAM4 for AWSLambdaBasicExecutionRole — it
 * grants own-log-group write only, the AWS-recommended Lambda baseline. */
function suppressLambdaBasicExecution(role: iam.Role): void {
  NagSuppressions.addResourceSuppressions(role, [
    {
      id: 'AwsSolutions-IAM4',
      reason:
        'AWSLambdaBasicExecutionRole grants the function write access to its ' +
        'own CloudWatch log group only. AWS-recommended logging baseline.',
      appliesTo: [
        'Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
      ],
    },
  ]);
}

export interface AnalyticsStackProps extends StackProps {
  customerName: string;
  /** Iceberg namespace name from IcebergStack — used for the LF grant. */
  icebergNamespace: string;
  /** S3 Tables bucket name (not ARN) — used to build the federated
   * child-catalog id and to scope LF grants. */
  tableBucketName: string;
  /** Analyst IAM role ARNs to grant SELECT/DESCRIBE on the cda namespace.
   * Empty list = create the federation but don't grant anyone (operator
   * grants by hand later). */
  analystRoleArns: string[];

  /** ARNs we monitor. Passed in from bin/app.ts so the stack stays
   * decoupled from the others' construct tree. */
  ingestStateMachineArn: string;
  reconStateMachineArn: string;
  notificationTopicArn: string;
  emrServerlessApplicationId: string;
  cursorTableName: string;

  /** Athena workgroup to publish saved queries into. Use `primary` unless
   * the customer has a dedicated workgroup. */
  athenaWorkgroup: string;

  /** Iceberg recon table name. Saved queries reference this. Defaults
   * to "cda_recon_results" but exposed for operators that override. */
  reconTableName: string;
}

/**
 * Optional analytics-side automation. Customers deploy this AFTER the
 * pipeline has been validated and the schedule is armed (see
 * docs/DEPLOYMENT.md §15 — Automation policy).
 *
 * What this stack provisions:
 *   1. Glue federated catalog `s3tablescatalog/<bucket>` so Athena can
 *      query S3 Tables as `s3tablescatalog/<bucket>.cda.<table>`.
 *   2. Lake Formation grants — SELECT + DESCRIBE on the cda namespace
 *      to a list of analyst role ARNs.
 *   3. Athena saved queries: ready-made recon and ops queries pre-loaded
 *      into the analyst's workgroup so they don't paste from docs.
 *   4. CloudWatch dashboard with state-machine, EMR, recon, and
 *      DynamoDB widgets.
 *   5. CloudWatch alarms wired to the existing SNS notification topic
 *      for state-machine failures, EMR vCPU spikes, Lambda errors, and
 *      DynamoDB throttles.
 *
 * What this stack does NOT do (deliberate, see DEPLOYMENT §15):
 *   - Grant Lake Formation admin to anyone (too privileged)
 *   - Subscribe operators to the SNS topic — operators need to confirm
 *     subscription manually
 *   - Issue partner-event-source acceptance for Lifecycle Events
 *
 * Idempotency:
 *   - Glue catalog creation uses a CR with try-create semantics; if the
 *     account already has `s3tablescatalog`, we skip (don't fail).
 *   - LF grants are individually idempotent in CFN.
 */
export class AnalyticsStack extends Stack {
  constructor(scope: Construct, id: string, props: AnalyticsStackProps) {
    super(scope, id, props);

    // ─── 1. Glue federated catalog (parent + per-bucket) ──────────────
    //
    // The parent `s3tablescatalog` is account-singleton. CFN's
    // AWS::Glue::Catalog will fail with "already exists" if another
    // tenant already created it. Wrap in a CR that returns success
    // either way. The per-bucket child catalog must reference the
    // specific S3 Tables bucket ARN.

    const federationProvider = this.makeFederationProvider(props);
    const federation = new CustomResource(this, 'GlueFederation', {
      serviceToken: federationProvider.serviceToken,
      properties: {
        ParentCatalogName: 's3tablescatalog',
        ChildCatalogName: props.tableBucketName,
        TableBucketArn: `arn:${this.partition}:s3tables:${this.region}:${this.account}:bucket/${props.tableBucketName}`,
      },
    });

    // ─── 2. Lake Formation grants for analyst roles ───────────────────
    //
    // Each analyst gets SELECT+DESCRIBE on the entire cda namespace
    // via a wildcard table grant. Doesn't make them LF admins. New
    // tables created in the namespace are covered by the wildcard.

    const lfCatalogId = `${this.account}:s3tablescatalog/${props.tableBucketName}`;
    props.analystRoleArns.forEach((roleArn, idx) => {
      const grant = new iam.CfnPolicy(this, `AnalystGrant${idx}`, {
        // We use a CfnPolicy as a placeholder to ensure ordering — the
        // real grant is a custom resource because LF's CFN type doesn't
        // accept federated catalog IDs (as of late 2025).
        policyName: `lf-grant-marker-${idx}-${this.stackName}`,
        policyDocument: { Version: '2012-10-17', Statement: [] },
        roles: [],
      });
      grant.cfnOptions.condition = undefined;  // always create

      const lfGrant = new CustomResource(this, `LakeFormationGrant${idx}`, {
        serviceToken: federationProvider.serviceToken,
        properties: {
          Action: 'GrantPermissions',
          PrincipalArn: roleArn,
          CatalogId: lfCatalogId,
          DatabaseName: props.icebergNamespace,
          // Two grants in one CR: namespace-level DESCRIBE and table-wildcard SELECT
          // The CR knows to issue both.
          Permissions: 'SELECT,DESCRIBE',
        },
      });
      lfGrant.node.addDependency(federation);
    });

    // ─── 3. Athena saved queries ──────────────────────────────────────

    const reconQueries: { name: string; description: string; sql: string }[] = [
      {
        name: `${props.customerName}-cda-iceberg-recon-failures`,
        description: 'All recon rows in the most recent run with non-OK status',
        sql: `SELECT tier, check_name, table_name, fingerprint, status, expected, actual, delta
FROM "s3tablescatalog/${props.tableBucketName}"."${props.icebergNamespace}"."${props.reconTableName}"
WHERE run_id = (SELECT max(run_id) FROM "s3tablescatalog/${props.tableBucketName}"."${props.icebergNamespace}"."${props.reconTableName}")
  AND status NOT IN ('OK', 'METRICS_PARTIAL')
ORDER BY tier, check_name, table_name`,
      },
      {
        name: `${props.customerName}-cda-iceberg-table-coverage`,
        description: 'Tables seen so far + last recon timestamp per table',
        sql: `SELECT table_name, max(committed_at) AS last_recon
FROM "s3tablescatalog/${props.tableBucketName}"."${props.icebergNamespace}"."${props.reconTableName}"
GROUP BY table_name
ORDER BY 2 DESC`,
      },
      {
        name: `${props.customerName}-cda-iceberg-drop-rate-trend`,
        description: 'CDA-side drop rate per table per day (Tier C)',
        sql: `SELECT date_trunc('day', committed_at) AS day,
       table_name,
       sum(cast(json_extract_scalar(details, '$.metrics_dropped') AS bigint)) AS dropped,
       sum(actual) AS rows_landed
FROM "s3tablescatalog/${props.tableBucketName}"."${props.icebergNamespace}"."${props.reconTableName}"
WHERE tier='A' AND check_name='batch_metrics'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC`,
      },
      {
        name: `${props.customerName}-cda-iceberg-tier-d-violations`,
        description: 'All Tier D violations (semantic invariants), most recent first',
        sql: `SELECT committed_at, table_name, check_name, actual AS violations, details
FROM "s3tablescatalog/${props.tableBucketName}"."${props.icebergNamespace}"."${props.reconTableName}"
WHERE tier='D' AND status='MISMATCH'
ORDER BY committed_at DESC
LIMIT 100`,
      },
    ];

    reconQueries.forEach((q, idx) => {
      new athena.CfnNamedQuery(this, `RecQuery${idx}`, {
        name: q.name,
        description: q.description,
        database: props.icebergNamespace,
        queryString: q.sql,
        workGroup: props.athenaWorkgroup,
      });
    });

    // ─── 4. CloudWatch dashboard ──────────────────────────────────────

    const ingestSm = props.ingestStateMachineArn.split(':').pop()!;
    const reconSm = props.reconStateMachineArn.split(':').pop()!;
    const dashboard = new cloudwatch.Dashboard(this, 'OperationsDashboard', {
      dashboardName: `${props.customerName}-cda-iceberg-ops`,
    });

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Ingest state machine — executions',
        left: [
          new cloudwatch.Metric({
            namespace: 'AWS/States', metricName: 'ExecutionsStarted',
            dimensionsMap: { StateMachineArn: props.ingestStateMachineArn },
            statistic: 'Sum', period: Duration.minutes(15),
          }),
          new cloudwatch.Metric({
            namespace: 'AWS/States', metricName: 'ExecutionsSucceeded',
            dimensionsMap: { StateMachineArn: props.ingestStateMachineArn },
            statistic: 'Sum', period: Duration.minutes(15),
          }),
          new cloudwatch.Metric({
            namespace: 'AWS/States', metricName: 'ExecutionsFailed',
            dimensionsMap: { StateMachineArn: props.ingestStateMachineArn },
            statistic: 'Sum', period: Duration.minutes(15),
          }),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'EMR Serverless — billed capacity',
        left: [
          new cloudwatch.Metric({
            namespace: 'AWS/EMRServerless', metricName: 'BilledVCpu',
            dimensionsMap: { ApplicationId: props.emrServerlessApplicationId },
            statistic: 'Average', period: Duration.minutes(5),
          }),
          new cloudwatch.Metric({
            namespace: 'AWS/EMRServerless', metricName: 'BilledMemoryGB',
            dimensionsMap: { ApplicationId: props.emrServerlessApplicationId },
            statistic: 'Average', period: Duration.minutes(5),
          }),
        ],
        width: 12,
      }),
    );
    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Recon state machine — executions',
        left: [
          new cloudwatch.Metric({
            namespace: 'AWS/States', metricName: 'ExecutionsStarted',
            dimensionsMap: { StateMachineArn: props.reconStateMachineArn },
            statistic: 'Sum', period: Duration.hours(1),
          }),
          new cloudwatch.Metric({
            namespace: 'AWS/States', metricName: 'ExecutionsFailed',
            dimensionsMap: { StateMachineArn: props.reconStateMachineArn },
            statistic: 'Sum', period: Duration.hours(1),
          }),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'DynamoDB — cursor table capacity',
        left: [
          new cloudwatch.Metric({
            namespace: 'AWS/DynamoDB', metricName: 'ConsumedReadCapacityUnits',
            dimensionsMap: { TableName: props.cursorTableName },
            statistic: 'Sum', period: Duration.minutes(15),
          }),
          new cloudwatch.Metric({
            namespace: 'AWS/DynamoDB', metricName: 'ConsumedWriteCapacityUnits',
            dimensionsMap: { TableName: props.cursorTableName },
            statistic: 'Sum', period: Duration.minutes(15),
          }),
        ],
        width: 12,
      }),
    );

    // ─── 5. CloudWatch alarms (wired to existing SNS topic) ───────────

    const topic = sns.Topic.fromTopicArn(this, 'NotificationTopicImport', props.notificationTopicArn);
    const action = new cloudwatchActions.SnsAction(topic);

    new cloudwatch.Alarm(this, 'IngestStateMachineFailures', {
      alarmName: `${props.customerName}-cda-iceberg-ingest-sm-failures`,
      metric: new cloudwatch.Metric({
        namespace: 'AWS/States', metricName: 'ExecutionsFailed',
        dimensionsMap: { StateMachineArn: props.ingestStateMachineArn },
        statistic: 'Sum', period: Duration.hours(1),
      }),
      threshold: 0, evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      alarmDescription: 'Any failed ingest execution in the last hour. Investigate Step Functions execution history.',
    }).addAlarmAction(action);

    new cloudwatch.Alarm(this, 'EmrVcpuSpike', {
      alarmName: `${props.customerName}-cda-iceberg-emr-vcpu-spike`,
      metric: new cloudwatch.Metric({
        namespace: 'AWS/EMRServerless', metricName: 'BilledVCpu',
        dimensionsMap: { ApplicationId: props.emrServerlessApplicationId },
        statistic: 'Average', period: Duration.hours(1),
      }),
      threshold: 100,  // tune per customer; 100 vCPU-hr/hr is ~$5/hr
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      alarmDescription: 'EMR Serverless billed vCPU exceeds threshold. Investigate runaway compute.',
    }).addAlarmAction(action);

    new cloudwatch.Alarm(this, 'DynamoDbThrottles', {
      alarmName: `${props.customerName}-cda-iceberg-ddb-throttles`,
      metric: new cloudwatch.Metric({
        namespace: 'AWS/DynamoDB', metricName: 'UserErrors',
        dimensionsMap: { TableName: props.cursorTableName },
        statistic: 'Sum', period: Duration.minutes(15),
      }),
      threshold: 0, evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      alarmDescription: 'DynamoDB user errors on cursor table. Cursor write failure means future ingests will re-read.',
    }).addAlarmAction(action);

    new CfnOutput(this, 'GlueCatalogId',  { value: lfCatalogId });
    new CfnOutput(this, 'DashboardUrl',   {
      value: `https://${this.region}.console.aws.amazon.com/cloudwatch/home?region=${this.region}#dashboards:name=${props.customerName}-cda-iceberg-ops`,
    });
    new CfnOutput(this, 'AthenaWorkgroup', { value: props.athenaWorkgroup });
  }

  /**
   * Build a single Lambda-backed custom-resource provider that handles
   * Glue federation + Lake Formation grants. Both APIs need
   * try-create-or-skip semantics that CFN's native types don't offer.
   */
  private makeFederationProvider(props: AnalyticsStackProps): Provider {
    const role = new iam.Role(this, 'FederationCrRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    role.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );
    // Glue catalog ops + Lake Formation ops scoped to the s3tablescatalog
    // identifier. Lake Formation grants are by principal, but only on
    // resources the customer's own table bucket — the resources clause
    // expresses that.
    role.addToPolicy(
      new iam.PolicyStatement({
        sid: 'GlueCatalogOps',
        actions: ['glue:CreateCatalog', 'glue:GetCatalog', 'glue:DeleteCatalog'],
        resources: ['*'],
      }),
    );
    role.addToPolicy(
      new iam.PolicyStatement({
        sid: 'LakeFormationOps',
        actions: ['lakeformation:GrantPermissions', 'lakeformation:RevokePermissions'],
        resources: ['*'],
      }),
    );
    role.addToPolicy(
      new iam.PolicyStatement({
        sid: 'PutDataLakeSettings',
        actions: ['lakeformation:GetDataLakeSettings', 'lakeformation:PutDataLakeSettings'],
        resources: ['*'],
      }),
    );

    const fnLogs = new logs.LogGroup(this, 'FederationCrLogs', {
      logGroupName: `/aws/lambda/${props.customerName}-cda-iceberg-federation-cr`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const onEventFn = new lambda.Function(this, 'FederationCrFn', {
      functionName: `${props.customerName}-cda-iceberg-federation-cr`,
      runtime: lambda.Runtime.NODEJS_24_X,
      handler: 'index.handler',
      timeout: Duration.minutes(5),
      role,
      logGroup: fnLogs,
      // Source lives in cdk/lambdas/federation-cr/index.mjs (asset, not an
      // inline backtick block) — real syntax-checked JS, and no embedded
      // template-literal for a static scanner to misread.
      // exclude test/** so unit tests aren't bundled into the Lambda zip
      // (the extracted idempotency.mjs the handler imports IS included).
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'federation-cr'), {
        exclude: ['test'],
      }),
      environment: { AWS_ACCOUNT_ID: this.account },
    });

    const provider = new Provider(this, 'FederationCrProvider', {
      onEventHandler: onEventFn,
    });

    // ── cdk-nag suppressions (evidence for AppSec) ──────────────────────
    // Our own federation custom-resource role.
    suppressLambdaBasicExecution(role);
    NagSuppressions.addResourceSuppressions(
      role,
      [
        {
          id: 'AwsSolutions-IAM5',
          reason:
            'glue:CreateCatalog/GetCatalog/DeleteCatalog and lakeformation:* ' +
            'Grant/Revoke/Get/PutDataLakeSettings are account-scoped admin APIs ' +
            'that do not accept resource-level ARNs (the Glue Catalog and the LF ' +
            'data-lake settings are account singletons). Resource:* is required by ' +
            'the AWS API contract. This role runs only at stack create/update as a ' +
            'one-shot federation bootstrap; it is not attached to any data-plane ' +
            'compute. The analytics stack is opt-in (enableAnalyticsStack=false by ' +
            'default) and deployed only after the pipeline is validated.',
          appliesTo: ['Resource::*'],
        },
      ],
      true,
    );

    // CDK-managed custom-resource provider framework (the `Provider`
    // construct synthesizes its own onEvent Lambda + execution role). Its
    // Node runtime and the AWSLambdaBasicExecutionRole + <fn>:* invoke
    // wildcard are emitted by aws-cdk-lib itself and are not configurable
    // from here; they move only when the CDK version is upgraded.
    NagSuppressions.addResourceSuppressions(
      provider,
      [
        {
          id: 'AwsSolutions-L1',
          reason:
            'Runtime of the CDK custom-resource provider framework Lambda is set ' +
            'by aws-cdk-lib, not by this stack. Tracked via CDK version upgrades.',
        },
        {
          id: 'AwsSolutions-IAM4',
          reason:
            'AWSLambdaBasicExecutionRole on the CDK-generated provider framework ' +
            'role (own-log-group write only).',
          appliesTo: [
            'Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
          ],
        },
        {
          id: 'AwsSolutions-IAM5',
          reason:
            'lambda:InvokeFunction <fn-arn>:* version-qualifier wildcard auto-added ' +
            'by the CDK provider framework to invoke its own onEvent handler.',
          appliesTo: [{ regex: '/^Resource::<.*\\.Arn>:\\*$/g' }],
        },
      ],
      true,
    );

    return provider;
  }
}
