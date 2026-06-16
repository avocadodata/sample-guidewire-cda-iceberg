// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import { Stack, StackProps, CfnOutput, Duration, RemovalPolicy } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cloudwatchActions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as ddb from 'aws-cdk-lib/aws-dynamodb';
import * as path from 'path';
import { NagSuppressions } from 'cdk-nag';
import { Construct } from 'constructs';

/**
 * Suppress cdk-nag AwsSolutions-IAM4 for the AWS-managed
 * AWSLambdaBasicExecutionRole on a Lambda execution role. That policy only
 * grants the function write access to its own CloudWatch log group — the
 * AWS-recommended logging baseline, not a broad data-plane grant.
 */
function suppressLambdaBasicExecution(role: iam.Role): void {
  NagSuppressions.addResourceSuppressions(role, [
    {
      id: 'AwsSolutions-IAM4',
      reason:
        'AWSLambdaBasicExecutionRole grants the function write access to its ' +
        'own CloudWatch log group only. AWS-recommended logging baseline; not ' +
        'a broad data-plane grant.',
      appliesTo: [
        'Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
      ],
    },
  ]);
}

export interface OrchestrationStackProps extends StackProps {
  customerName: string;

  emrServerlessApplicationId: string;
  emrJobRoleArn: string;

  /** S3 Tables bucket ARN — passed as a spark conf to the per-table jobs. */
  icebergTableBucketArn: string;
  /** Iceberg namespace under which all CDA tables live. */
  icebergNamespace: string;

  /** CDA writer S3 bucket ARN. The launch-condition Lambda reads the manifest. */
  cdaSourceBucketArn: string;
  cdaManifestKey: string;
  tablesToExclude: string;
  /** Column exclusion spec passed to the Spark job as COLUMNS_TO_EXCLUDE.
   * Comma-separated, NO spaces (spark-submit splits on whitespace).
   * Entries: `col` (all tables) or `table:col` (one table). Empty = none.
   * e.g. "ssn,taxid,cc_claim:description" */
  columnsToExclude: string;

  /** Pre-computed bucket names for spark-submit args. */
  bucketNames: { artifact: string; logs: string };

  cronExpression: string;
  scheduleEnabled: boolean;
  notificationEmails: string;

  /** Map state concurrency (parallel per-table jobs). */
  mapStateConcurrency: number;
  /** Percentage of per-table failures the ingest + recon Maps tolerate
   * before the whole execution is marked FAILED. Default 0 = strict: one
   * failed table fails the execution (loud, production-correct). Set >0
   * (e.g. 10) for a resilient bulk load where a few transient/known table
   * failures shouldn't fail the run — completeness is then proven by
   * reconciliation + the per-table report, not by execution status. */
  mapToleratedFailurePercentage: number;
  /** EMR Serverless job-run poll interval (seconds). */
  serverlessPollIntervalSeconds: number;
  /** Spark/Iceberg runtime versions. Used to build the deterministic jar
   * filenames staged under s3://<artifact>/deps/ by data_load.sh's ship_deps
   * (gradle stageRuntimeDeps). When useMavenPackages=true they instead render
   * the --packages Maven coordinates. */
  sparkScalaVersion: string;
  icebergRuntimeVersion: string;
  s3TablesCatalogVersion: string;
  /** Escape hatch: when true, EMR jobs resolve the Iceberg + S3 Tables runtime
   * via spark-submit `--packages` (Maven Central) instead of `--jars` against
   * the pre-staged S3 copies. Default false (use S3-staged jars). Maven
   * resolution at job start is unreliable under Map concurrency — simultaneous
   * jobs hammer Maven Central and a fraction get transient "module not found"
   * failures that kill the job before any work runs. Only flip this true if
   * the deps weren't staged (legacy deploy) or for a one-off debug. */
  useMavenPackages: boolean;

  /** Per-table size-class thresholds (row counts). Values >= medium ride
   * the medium-class spark conf; >= large ride the large-class conf. */
  sizeThresholdMedium: number;
  sizeThresholdLarge: number;

  /** CloudWatch Logs retention for all log groups created by this stack
   * (Lambda function logs, Step Functions execution logs). Pass an integer
   * day count; the nearest valid CloudWatch RetentionDays enum value is
   * used. Common: 7, 30, 90, 365, 731 (2y), 1827 (5y), 2557 (7y). */
  logRetentionDays: number;

  /** Tier D recon schedule. Recon is a separate scheduled state machine
   * (cda-iceberg-recon) that runs the IcebergIngest jar in --recon-only
   * mode against every table in the manifest. Heavier than ingest (full
   * scans of raw+merged) so the default cadence is much lower. */
  reconCronExpression: string;
  reconScheduleEnabled: boolean;
  reconMapConcurrency: number;

  /** CDA Lifecycle Events (EAP feature) — opt-in event-driven trigger
   * for the ingest state machine on every cda.streamingBatchCompleted
   * event from CDA's EventBridge partner source. When enabled, complements
   * (not replaces) the cron schedule — operators can run both for
   * defense-in-depth or disable cron entirely. */
  lifecycleEventsEnabled: boolean;
  /** Partner event source name (e.g. "aws.partner/guidewire.com/<customer-id>/<source-name>").
   * Empty string when lifecycleEventsEnabled=false. */
  lifecyclePartnerEventSource: string;
  /** Filter to a specific InsuranceSuite app's events ("cc", "pc", "bc",
   * or "" for all). */
  lifecycleSourceApp: string;
}

/** Spark conf knobs that vary by size class. Each key here lands as one
 * `--conf <key>=<value>` pair in spark-submit. */
interface SizeClassConf {
  driverCores: string;
  driverMemory: string;
  executorCores: string;
  executorMemory: string;
  executorDisk: string;
  shufflePartitions: string;
}

const SIZE_CONFS: Record<'small' | 'medium' | 'large', SizeClassConf> = {
  // EMR Serverless defaults are fine for tiny typecodes; just set sane
  // numbers so spark-submit doesn't pick whatever the cluster's default is.
  small: {
    driverCores: '2',  driverMemory: '4G',
    executorCores: '4', executorMemory: '12G',
    executorDisk: '20G', shufflePartitions: '50',
  },
  // Mid-volume CDA tables (millions to ~1B rows). 200 GB local disk is the
  // EMR Serverless max for the standard worker class; 400 partitions keeps
  // per-partition shuffle spill manageable.
  medium: {
    driverCores: '4',  driverMemory: '12G',
    executorCores: '4', executorMemory: '16G',
    executorDisk: '200G', shufflePartitions: '400',
  },
  // Multi-billion-row tables. Bigger driver for catalyst plan, fatter
  // executors so fewer of them are needed (cuts shuffle messages),
  // 400 GB disk via EMR Serverless's larger worker class request, and
  // 1000 shuffle partitions so each handles ~1.5 GB of spill at most.
  large: {
    driverCores: '8',  driverMemory: '24G',
    executorCores: '8', executorMemory: '32G',
    executorDisk: '400G', shufflePartitions: '1000',
  },
};

/** Map an integer day count to the nearest CloudWatch RetentionDays
 * enum value at-or-below it. CloudWatch only accepts specific durations
 * (1, 3, 5, 7, 14, 30, ... 3653) so we snap conservatively — never longer
 * than the operator asked for, never longer than the customer's
 * compliance / cost ceiling. */
function snapRetention(days: number): logs.RetentionDays {
  const allowed: Array<[number, logs.RetentionDays]> = [
    [1, logs.RetentionDays.ONE_DAY],
    [3, logs.RetentionDays.THREE_DAYS],
    [5, logs.RetentionDays.FIVE_DAYS],
    [7, logs.RetentionDays.ONE_WEEK],
    [14, logs.RetentionDays.TWO_WEEKS],
    [30, logs.RetentionDays.ONE_MONTH],
    [60, logs.RetentionDays.TWO_MONTHS],
    [90, logs.RetentionDays.THREE_MONTHS],
    [120, logs.RetentionDays.FOUR_MONTHS],
    [150, logs.RetentionDays.FIVE_MONTHS],
    [180, logs.RetentionDays.SIX_MONTHS],
    [365, logs.RetentionDays.ONE_YEAR],
    [400, logs.RetentionDays.THIRTEEN_MONTHS],
    [545, logs.RetentionDays.EIGHTEEN_MONTHS],
    [731, logs.RetentionDays.TWO_YEARS],
    [1827, logs.RetentionDays.FIVE_YEARS],
    [2192, logs.RetentionDays.SIX_YEARS],
    [2557, logs.RetentionDays.SEVEN_YEARS],
    [2922, logs.RetentionDays.EIGHT_YEARS],
    [3288, logs.RetentionDays.NINE_YEARS],
    [3653, logs.RetentionDays.TEN_YEARS],
  ];
  let pick = logs.RetentionDays.ONE_MONTH;
  for (const [d, enumVal] of allowed) {
    if (d <= days) pick = enumVal;
    else break;
  }
  return pick;
}

/**
 * Step Functions orchestration for the Iceberg ingest path.
 *
 *   CheckCDAChanges (Lambda + DynamoDB)
 *      └─ catch → NotifyLaunchConditionFailure → Fail
 *   Choice on $.status
 *      ├─ START   → Map(per changedTable: SubmitJob → Wait → GetJobRun → ...)
 *      │           → NotifyCompletion → Succeed
 *      │           (the Spark job advances both cursors and the HWM; no
 *      │            post-Map bookmark Lambda)
 *      └─ default → NotifyNoRun → Succeed
 *
 * The Map state runs up to `mapStateConcurrency` per-table EMR Serverless
 * jobs in parallel. Each iterator drives its own StartJobRun + poll loop.
 * If any one table fails, the whole Map fails (default behavior — we want
 * to know about partial successes rather than silently succeeding).
 *
 * Why a Map per-table instead of one big job (like OSR):
 *   - Smaller blast radius: a single bad table can't OOM the whole batch.
 *   - Per-table executor sizing: Spark cores/memory can be tuned per table
 *     class (small/medium/large) without touching code.
 *   - Iceberg MERGE per-table is a single SQL — no need to drive 717
 *     tables through one driver process.
 */
export class OrchestrationStack extends Stack {
  readonly stateMachine: sfn.StateMachine;
  readonly notificationTopic: sns.Topic;
  /** Tier D recon state machine. Exposed so AnalyticsStack can monitor it. */
  reconStateMachine!: sfn.StateMachine;
  /** DDB cursor table name. Exposed so AnalyticsStack can monitor it. */
  cursorTableName!: string;
  /** CloudWatch Log Group the EMR jobs stream driver/executor logs to (in
    * addition to S3). Lets us metric-filter on structured markers like
    * RECON_WRITE_FAILED. */
  private emrLogGroupName!: string;

  constructor(scope: Construct, id: string, props: OrchestrationStackProps) {
    super(scope, id, props);

    if (!props.cdaSourceBucketArn) {
      // String concatenation (not a template literal) + no angle-bracket
      // placeholder, so the message isn't misread as HTML by static scanners.
      throw new Error(
        id + ': cdaSourceBucketArn is required. Pass ' +
        '--context cdaSourceBucketArn=arn:aws:s3:::your-bucket-name',
      );
    }
    if (!props.emrServerlessApplicationId) {
      throw new Error(`${id}: emrServerlessApplicationId is required.`);
    }

    const sourceBucketName = arnToBucketName(props.cdaSourceBucketArn);
    const manifestObjectArn = `${props.cdaSourceBucketArn}/${props.cdaManifestKey}`;

    // --- DynamoDB state table for per-table change detection -----------
    const stateTable = new ddb.Table(this, 'LaunchConditionState', {
      tableName: `${props.customerName}-cda-iceberg-state`,
      partitionKey: { name: 'tableName', type: ddb.AttributeType.STRING },
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
      encryption: ddb.TableEncryption.AWS_MANAGED,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      removalPolicy: RemovalPolicy.RETAIN,
    });
    this.cursorTableName = stateTable.tableName;

    // The Spark job advances per-(table, fingerprint) cursors directly so
    // each fingerprint commit is its own atomic checkpoint. Without this
    // a crash mid-table would lose progress on already-committed fingerprints.
    // Scoped UpdateItem only — Spark doesn't read the bookmark or write to
    // any other DDB resource.
    iam.Role.fromRoleArn(this, 'EmrJobRoleForCursorAdvance', props.emrJobRoleArn).addToPrincipalPolicy(
      new iam.PolicyStatement({
        sid: 'AdvanceFingerprintCursor',
        actions: ['dynamodb:UpdateItem', 'dynamodb:GetItem'],
        resources: [stateTable.tableArn],
      }),
    );

    // --- Launch-condition Lambda (read-only) ----------------------------
    const launchRole = new iam.Role(this, 'LaunchConditionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      description: `${props.customerName} CDA Iceberg launch-condition role`,
    });
    launchRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );
    launchRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'ReadManifestObjectOnly',
        actions: ['s3:GetObject'],
        resources: [manifestObjectArn],
      }),
    );
    stateTable.grantReadData(launchRole);

    const launchLogGroup = new logs.LogGroup(this, 'LaunchConditionLogs', {
      logGroupName: `/aws/lambda/${props.customerName}-cda-iceberg-launch-condition`,
      retention: snapRetention(props.logRetentionDays),
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const launchFn = new lambda.Function(this, 'LaunchConditionFn', {
      functionName: `${props.customerName}-cda-iceberg-launch-condition`,
      runtime: lambda.Runtime.NODEJS_24_X,
      handler: 'index.handler',
      // exclude test/** so unit tests aren't bundled (decide.mjs/detect-reset.mjs
      // the handler imports ARE included).
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'launch-condition'), {
        exclude: ['test'],
      }),
      memorySize: 256,
      timeout: Duration.minutes(2),
      role: launchRole,
      environment: {
        SOURCE_BUCKET: sourceBucketName,
        MANIFEST_KEY: props.cdaManifestKey,
        STATE_TABLE: stateTable.tableName,
        TABLES_TO_EXCLUDE: props.tablesToExclude,
        SIZE_THRESHOLD_MEDIUM: String(props.sizeThresholdMedium),
        SIZE_THRESHOLD_LARGE:  String(props.sizeThresholdLarge),
      },
      description: 'Reads CDA manifest, diffs against per-table state table, returns changed-table list.',
      logGroup: launchLogGroup,
    });

    // NOTE: there is no post-Map "advance-state" Lambda. The Spark job is
    // the single authoritative writer of both fingerprintCursors and the
    // table-level lastSuccessfulWriteTimestamp (HWM), advancing the HWM
    // from the manifest snapshot it read at job time. A post-Map Lambda
    // would lag the cursors (it'd use launch-condition's older manifest
    // read), which at CDA's 90-120s update cadence caused spurious
    // re-dispatch every cycle. The EMR job role already has UpdateItem on
    // the cursor table (granted below) which covers the HWM write.

    // --- SNS notification topic -----------------------------------------
    this.notificationTopic = new sns.Topic(this, 'NotificationTopic', {
      topicName: `${props.customerName}-cda-iceberg-notifications`,
      displayName: `${props.customerName} CDA Iceberg notifications`,
    });
    this.notificationTopic.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: 'EnforceTLS',
        effect: iam.Effect.DENY,
        principals: [new iam.AnyPrincipal()],
        actions: ['sns:Publish'],
        resources: [this.notificationTopic.topicArn],
        conditions: { Bool: { 'aws:SecureTransport': 'false' } },
      }),
    );
    for (const email of props.notificationEmails.split(',').map((s) => s.trim()).filter(Boolean)) {
      this.notificationTopic.addSubscription(new subs.EmailSubscription(email));
    }

    // --- EMR log group + recon-failure alarm ----------------------------
    // EMR Serverless jobs stream logs to S3 by default; we ALSO send them to
    // this CloudWatch Log Group so a metric filter can watch for structured
    // markers. The Spark job emits `RECON_WRITE_FAILED ...` (log.error) when
    // a Tier A/B recon write throws — that failure is non-fatal to ingest
    // (the job still SUCCEEDS), so without this it would be invisible (it
    // once left cda_recon_results empty for a whole load). The EMR job role
    // already has logs:CreateLogStream/PutLogEvents on /aws/emr-serverless/*.
    const emrLogGroup = new logs.LogGroup(this, 'EmrAppLogs', {
      logGroupName: `/aws/emr-serverless/${props.customerName}-cda-iceberg`,
      retention: snapRetention(props.logRetentionDays),
      removalPolicy: RemovalPolicy.DESTROY,
    });
    this.emrLogGroupName = emrLogGroup.logGroupName;

    const reconFailMetric = emrLogGroup.addMetricFilter('ReconWriteFailedFilter', {
      filterPattern: logs.FilterPattern.literal('RECON_WRITE_FAILED'),
      metricNamespace: `${props.customerName}-cda-iceberg`,
      metricName: 'ReconWriteFailed',
      metricValue: '1',
      defaultValue: 0,
    });
    new cloudwatch.Alarm(this, 'ReconWriteFailedAlarm', {
      alarmName: `${props.customerName}-cda-iceberg-recon-write-failed`,
      alarmDescription:
        'A Spark job logged RECON_WRITE_FAILED — a Tier A/B recon write was ' +
        'swallowed (ingest still succeeded). cda_recon_results may be missing ' +
        'rows. Investigate the EMR driver logs for the named table.',
      metric: reconFailMetric.metric({ statistic: 'Sum', period: Duration.minutes(5) }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    }).addAlarmAction(new cloudwatchActions.SnsAction(this.notificationTopic));

    // --- State machine pieces ------------------------------------------
    const checkCdaChanges = new tasks.LambdaInvoke(this, 'CheckCDAChanges', {
      lambdaFunction: launchFn,
      outputPath: '$.Payload',
      retryOnServiceExceptions: true,
    });

    const errorPayload = sfn.TaskInput.fromObject({
      Error: sfn.JsonPath.stringAt('$.Error'),
      Cause: sfn.JsonPath.stringAt('$.Cause'),
    });

    const notifyLaunchFailure = new tasks.SnsPublish(this, 'NotifyLaunchConditionFailure', {
      topic: this.notificationTopic,
      subject: `${props.customerName} CDA Iceberg launch-condition failed`,
      message: errorPayload,
      resultPath: sfn.JsonPath.DISCARD,
    }).next(new sfn.Fail(this, 'LaunchConditionFailed'));

    const notifyMapFailure = new tasks.SnsPublish(this, 'NotifyMapFailure', {
      topic: this.notificationTopic,
      subject: `${props.customerName} CDA Iceberg per-table run failed`,
      message: errorPayload,
      resultPath: sfn.JsonPath.DISCARD,
    }).next(new sfn.Fail(this, 'MapFailed'));

    const notifyCompletion = new tasks.SnsPublish(this, 'NotifyCompletion', {
      topic: this.notificationTopic,
      subject: `${props.customerName} CDA Iceberg run completed`,
      message: sfn.TaskInput.fromText('All per-table jobs succeeded.'),
      resultPath: sfn.JsonPath.DISCARD,
    }).next(new sfn.Succeed(this, 'RunSucceeded'));

    const notifyNoRun = new tasks.SnsPublish(this, 'NotifyNoRun', {
      topic: this.notificationTopic,
      subject: `${props.customerName} CDA Iceberg — no changes`,
      message: sfn.TaskInput.fromText(
        'Launch conditions not met (no manifest changes since last successful run).',
      ),
      resultPath: sfn.JsonPath.DISCARD,
    }).next(new sfn.Succeed(this, 'NoChangesDetected'));

    // CDA_RESET branch: high-priority alert, no ingest. Fires when the
    // launch-condition Lambda detects that CDA's manifest has gone
    // backwards or pruned a fingerprint we have a cursor for. Continuing
    // to ingest with stale cursors against a re-deployed source bucket
    // would silently produce inconsistent data; the operator must
    // intervene (drop+rebuild affected tables or wait for CDA's bulk
    // load to complete and clear cursors). The Fail terminal lets a
    // future EventBridge rule fan out to PagerDuty / OpsGenie if desired.
    const notifyCdaReset = new tasks.SnsPublish(this, 'NotifyCdaReset', {
      topic: this.notificationTopic,
      subject: `${props.customerName} CDA Iceberg — CDA RESET DETECTED — ingest paused`,
      message: sfn.TaskInput.fromObject({
        message: 'CDA manifest has regressed (HWM rolled back or fingerprint pruned). ' +
                 'Ingest paused. Inspect the resets[] array for affected tables.',
        'resets.$': '$.resets',
      }),
      resultPath: sfn.JsonPath.DISCARD,
    }).next(new sfn.Fail(this, 'CdaResetDetected', {
      error: 'CdaReset',
      cause: 'Launch-condition Lambda returned status=CDA_RESET',
    }));

    checkCdaChanges.addCatch(notifyLaunchFailure, { errors: [sfn.Errors.ALL] });

    // --- Per-table iterator ---------------------------------------------
    // Each Map iteration receives an item: { tableName, ts }. The iterator
    // submits one StartJobRun (with the tableName as a spark-submit arg),
    // polls until terminal, and returns the success state up to the Map.
    const iterator = this.buildPerTableIterator(props, sourceBucketName);

    // DISTRIBUTED Map (not inline): a 717-table inline Map runs every
    // per-table poll loop (Wait→GetJobRun→Choice, ~60s cadence for minutes)
    // in the PARENT execution history, which blows the hard 25,000
    // history-event limit partway through a full load (States.Runtime).
    // A Distributed Map runs each table as its OWN child STANDARD execution
    // with its own 25K budget, so the parent only records one event per
    // child. mapExecutionType=STANDARD (NOT express) because big-table jobs
    // run >5 min. toleratedFailurePercentage is NATIVE here (no escape
    // hatch needed). itemSelector + $$.Map.Item.Value context syntax are
    // unchanged from the inline Map. CDK auto-grants the role
    // states:StartExecution/DescribeExecution/StopExecution for the children.
    const mapState = new sfn.DistributedMap(this, 'PerTableMap', {
      itemsPath: '$.changedTables',
      maxConcurrency: props.mapStateConcurrency,
      mapExecutionType: sfn.StateMachineType.STANDARD,
      toleratedFailurePercentage: props.mapToleratedFailurePercentage || undefined,
      // Carry sourceBucket+manifestKey so the iterator can pass them as args.
      itemSelector: {
        'tableName.$': '$$.Map.Item.Value.tableName',
        'ts.$':        '$$.Map.Item.Value.ts',
        // sizeClass drives the per-iteration TableSizeClass Choice (small/
        // medium/large spark conf). Must be passed through here or the
        // Choice fails with "invalid path '$.sizeClass'".
        'sizeClass.$': '$$.Map.Item.Value.sizeClass',
        sourceBucket:  sourceBucketName,
        manifestKey:   props.cdaManifestKey,
      },
      resultPath: '$.mapResults',
    });
    mapState.itemProcessor(iterator);
    mapState.addCatch(notifyMapFailure, { errors: [sfn.Errors.ALL] });

    // No post-Map bookmark step. The Spark job is the single authoritative
    // writer of both fingerprintCursors AND lastSuccessfulWriteTimestamp:
    // it advances the HWM from the manifest snapshot IT read at job time
    // (minutes after launch-condition's read), so the HWM stays consistent
    // with the cursors. A post-Map Lambda would use launch-condition's
    // older manifest read and lag the cursors — at CDA's 90-120s update
    // cadence that lag caused spurious re-dispatch every cycle. See
    // IcebergIngest.run's advanceHighWaterMark calls.
    const startBranch = mapState.next(notifyCompletion);

    const decideStart = new sfn.Choice(this, 'CanCDACStart?')
      .when(sfn.Condition.stringEquals('$.status', 'CDA_RESET'), notifyCdaReset)
      .when(sfn.Condition.stringEquals('$.status', 'START'), startBranch)
      .otherwise(notifyNoRun);

    const definition = checkCdaChanges.next(decideStart);

    // --- State machine role ---------------------------------------------
    const smRole = new iam.Role(this, 'StateMachineRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
      description: `${props.customerName} CDA Iceberg state machine execution role`,
    });
    smRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'PassEmrJobRoleToServerless',
        actions: ['iam:PassRole'],
        resources: [props.emrJobRoleArn],
        conditions: {
          StringEquals: { 'iam:PassedToService': 'emr-serverless.amazonaws.com' },
        },
      }),
    );

    const smLogs = new logs.LogGroup(this, 'StateMachineLogs', {
      retention: snapRetention(props.logRetentionDays),
      removalPolicy: RemovalPolicy.DESTROY,
    });

    this.stateMachine = new sfn.StateMachine(this, 'StateMachine', {
      stateMachineName: `${props.customerName}-cda-iceberg-orchestration`,
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      role: smRole,
      logs: {
        destination: smLogs,
        level: sfn.LogLevel.ALL,
        includeExecutionData: true,
      },
      tracingEnabled: true,
    });

    new events.Rule(this, 'ScheduleRule', {
      ruleName: `${props.customerName}-cda-iceberg-orchestration`,
      schedule: events.Schedule.expression(`cron(${props.cronExpression})`),
      enabled: props.scheduleEnabled,
      targets: [new eventsTargets.SfnStateMachine(this.stateMachine)],
    });

    // CDA Lifecycle Events (EAP) — event-driven trigger.
    // Architecturally:
    //   1. EventBridge custom bus, associated with CDA's partner event
    //      source name. The association is what lets events from CDA
    //      flow into our account. Operator must already have accepted
    //      the partner-source invitation in their EventBridge console
    //      before deploy (CFN can't do the accept; it's a one-time UI step).
    //   2. Rule on that bus matching com.guidewire.cda.streamingBatchCompleted
    //      (and optionally filtering on sourceInsuranceSuiteApp).
    //   3. Target = the ingest state machine. Each event → one execution.
    //
    // Doesn't replace the cron schedule; both can be on at once. Idle
    // state machine executions are cheap (Lambda diff returns STOP if
    // there's nothing new), so duplicate triggers are safe.
    if (props.lifecycleEventsEnabled) {
      if (!props.lifecyclePartnerEventSource) {
        throw new Error(
          `${id}: lifecycleEventsEnabled=true requires lifecyclePartnerEventSource ` +
          `(get the partner source name from Guidewire when joining the EAP).`,
        );
      }
      // Partner-source-associated event buses derive their name from the
      // source name; eventBusName must NOT be specified.
      const lifecycleBus = new events.EventBus(this, 'LifecycleEventBus', {
        eventSourceName: props.lifecyclePartnerEventSource,
      });

      const detail: Record<string, unknown> = {
        type: ['com.guidewire.cda.streamingBatchCompleted'],
      };
      if (props.lifecycleSourceApp) {
        detail.sourceInsuranceSuiteApp = [props.lifecycleSourceApp];
      }
      // The doc's recommended source matcher is:
      //   [{prefix: "aws.partner/"}, {anything-but: {prefix: "aws."}}]
      // CDK's Match.prefix / Match.anythingButPrefix each return a
      // length-1 array of those JSON shapes; concat them to build the
      // composite match.
      const sourceMatchers: string[] = [
        ...events.Match.prefix('aws.partner/'),
        ...events.Match.anythingButPrefix('aws.'),
      ];
      new events.Rule(this, 'LifecycleEventsRule', {
        ruleName: `${props.customerName}-cda-iceberg-lifecycle`,
        eventBus: lifecycleBus,
        eventPattern: {
          source: sourceMatchers,
          detail,
        },
        // Each lifecycle event → one ingest execution. The Lambda's
        // change-detection short-circuits to STOP if nothing's new, so
        // duplicate firings (e.g. from cron + lifecycle) are no-ops.
        targets: [new eventsTargets.SfnStateMachine(this.stateMachine)],
      });
    }

    // Tier D recon — separate state machine + schedule. Doesn't share
    // the launch-condition gate (recon always runs when triggered) or
    // the AdvanceState lambda (recon doesn't move bookmarks).
    const reconSm = this.buildReconStateMachine(props, sourceBucketName, smRole);
    this.reconStateMachine = reconSm;

    new CfnOutput(this, 'StateMachineArn',           { value: this.stateMachine.stateMachineArn });
    new CfnOutput(this, 'ReconStateMachineArn',      { value: reconSm.stateMachineArn });
    new CfnOutput(this, 'NotificationTopicArn',      { value: this.notificationTopic.topicArn });
    new CfnOutput(this, 'LaunchConditionStateTable', { value: stateTable.tableName });
    new CfnOutput(this, 'LaunchConditionFunctionName', { value: launchFn.functionName });

    this.applyNagSuppressions(launchRole, smRole);
  }

  // ── cdk-nag suppressions (evidence for AppSec) ────────────────────────
  // Grouped here so the rationale for every accepted finding is in one
  // place. Everything that could be fixed in code (Lambda runtime, S3
  // access logging, VPC flow logs, Step Functions logging + X-Ray) already
  // is — these are the residual findings that are either AWS-mandated
  // wildcards or the standard Lambda execution-role managed policy.
  private applyNagSuppressions(
    launchRole: iam.Role,
    smRole: iam.Role,
  ): void {
    // AWSLambdaBasicExecutionRole on the launch-condition Lambda role. (The
    // recon-list Lambda role gets the same suppression inside
    // buildReconStateMachine, where it is in scope.)
    suppressLambdaBasicExecution(launchRole);

    // State machine execution role. Three classes of accepted wildcard:
    NagSuppressions.addResourceSuppressions(
      smRole,
      [
        {
          id: 'AwsSolutions-IAM5',
          reason:
            'lambda:InvokeFunction <fn-arn>:* — the trailing :* is the function ' +
            'VERSION/alias qualifier that CDK appends automatically. The function ' +
            'ARNs themselves (launch-condition, recon-list) are pinned; this does ' +
            'not widen invocation beyond those two functions.',
          appliesTo: [
            { regex: '/^Resource::<.*\\.Arn>:\\*$/g' },
          ],
        },
        {
          id: 'AwsSolutions-IAM5',
          reason:
            'emr-serverless:GetJobRun on /applications/<app-id>/jobruns/* — job-run ' +
            'IDs are generated by EMR at StartJobRun time and are not knowable at ' +
            'deploy time. Scoped to this account, region, and the single EMR ' +
            'application; cannot read job runs of any other application.',
          // Anchored to the full emr-serverless application/jobruns ARN so it
          // only matches the GetJobRun grant, not any other "*"-suffixed
          // resource that might be added to this role later.
          appliesTo: [
            { regex: '/^Resource::arn:aws:emr-serverless:.*:\\/applications\\/.*\\/jobruns\\/\\*$/g' },
          ],
        },
        {
          id: 'AwsSolutions-IAM5',
          reason:
            'CloudWatch Logs log-delivery + X-Ray trace APIs require Resource:* by ' +
            'AWS design — these actions (CreateLogDelivery, PutTraceSegments, etc.) ' +
            'have no resource-level ARN to scope to. This statement is generated by ' +
            'the CDK StateMachine construct when logging+tracing are enabled (both ' +
            'of which are security improvements we deliberately turned on).',
          appliesTo: [
            'Resource::*',
          ],
        },
      ],
      true, // applyToChildren — DefaultPolicy is a child of the role
    );

    // Distributed Map child-execution policy. Both state machines run their
    // per-table Map in DISTRIBUTED mode, so CDK adds a DistributedMapPolicy
    // granting states:StartExecution / DescribeExecution / StopExecution on
    // the MACHINE'S OWN child executions (arn:...:execution:<this-sm>:* and
    // .../*:*). The child-execution IDs are generated by Step Functions at
    // run time and are not knowable at deploy time; the ARN is scoped to
    // THIS state machine's own executions only. Required by the Distributed
    // Map service integration — there is no narrower form.
    for (const sm of [this.stateMachine, this.reconStateMachine]) {
      NagSuppressions.addResourceSuppressions(
        sm,
        [
          {
            id: 'AwsSolutions-IAM5',
            reason:
              'Distributed Map child-execution grant (states:StartExecution/' +
              'DescribeExecution/StopExecution). Child execution IDs are ' +
              'runtime-generated; the ARN is scoped to this state machine\'s ' +
              'own executions. AWS-required for Distributed Map; no narrower form.',
            appliesTo: [
              { regex: '/^Resource::arn:aws:states:.*:execution:.*$/g' },
            ],
          },
        ],
        true, // applyToChildren — DistributedMapPolicy is a child of the SM
      );
    }
  }

  // ---------------------------------------------------------------------
  // Tier D recon state machine. Architecturally simpler than ingest:
  //   ListTables (Lambda — manifest dump) → Map(per table:
  //     StartJobRun(--recon-only) → Wait → GetJobRun → Choice).
  // No DDB cursor reads, no AdvanceState, no launch-condition gate.
  // ---------------------------------------------------------------------
  private buildReconStateMachine(
    props: OrchestrationStackProps,
    sourceBucketName: string,
    smRole: iam.Role,
  ): sfn.StateMachine {
    // List-tables Lambda: read manifest, return [{tableName, ...}, ...]
    const reconListRole = new iam.Role(this, 'ReconListRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      description: `${props.customerName} CDA Iceberg recon-list role`,
    });
    reconListRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );
    reconListRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'ReadManifestObjectOnly',
        actions: ['s3:GetObject'],
        resources: [`${props.cdaSourceBucketArn}/${props.cdaManifestKey}`],
      }),
    );

    suppressLambdaBasicExecution(reconListRole);

    const reconListLogGroup = new logs.LogGroup(this, 'ReconListLogs', {
      logGroupName: `/aws/lambda/${props.customerName}-cda-iceberg-recon-list`,
      retention: snapRetention(props.logRetentionDays),
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const reconListFn = new lambda.Function(this, 'ReconListFn', {
      functionName: `${props.customerName}-cda-iceberg-recon-list`,
      runtime: lambda.Runtime.NODEJS_24_X,
      handler: 'index.handler',
      // exclude test/** so unit tests aren't bundled (select-tables.mjs the
      // handler imports IS included).
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'recon-list'), {
        exclude: ['test'],
      }),
      memorySize: 256,
      timeout: Duration.minutes(2),
      role: reconListRole,
      environment: {
        SOURCE_BUCKET: sourceBucketName,
        MANIFEST_KEY: props.cdaManifestKey,
        TABLES_TO_EXCLUDE: props.tablesToExclude,
      },
      description: 'Reads CDA manifest and returns all table names for Tier D recon fan-out.',
      logGroup: reconListLogGroup,
    });

    const listTables = new tasks.LambdaInvoke(this, 'ReconListTables', {
      lambdaFunction: reconListFn,
      outputPath: '$.Payload',
      retryOnServiceExceptions: true,
    });

    // Per-table iterator: one StartJobRun per Map iteration. Recon is
    // homogeneous in cost (4 SQL scans per table) so we don't need
    // size-class branching here — one set of medium-class spark conf
    // for everyone keeps the state machine compact.
    const reconStart = this.makeReconStartJobRun(props);
    void sourceBucketName;  // recon doesn't read source parquet

    const wait = new sfn.Wait(this, 'WaitForReconJobRun', {
      time: sfn.WaitTime.duration(Duration.seconds(props.serverlessPollIntervalSeconds)),
    });
    const getJobRun = new tasks.CallAwsService(this, 'GetReconJobRun', {
      service: 'emrserverless',
      action: 'getJobRun',
      parameters: {
        ApplicationId: props.emrServerlessApplicationId,
        JobRunId: sfn.JsonPath.stringAt('$.startResult.JobRunId'),
      },
      iamResources: [
        `arn:${this.partition}:emr-serverless:${this.region}:${this.account}:/applications/${props.emrServerlessApplicationId}/jobruns/*`,
      ],
      iamAction: 'emr-serverless:GetJobRun',
      resultPath: '$.jobRun',
    });
    const succeed = new sfn.Pass(this, 'ReconTableSucceeded', {
      parameters: {
        'tableName.$': '$.tableName',
        'jobRunId.$': '$.startResult.JobRunId',
        state: 'SUCCESS',
      },
    });
    const failPass = new sfn.Fail(this, 'ReconTableJobRunFailed', {
      errorPath: sfn.JsonPath.format(
        'ReconJobFailed:{}',
        sfn.JsonPath.stringAt('$.tableName'),
      ),
      causePath: sfn.JsonPath.stringAt('$.jobRun.JobRun.StateDetails'),
    });
    const decide = new sfn.Choice(this, 'ReconJobRunState')
      .when(
        sfn.Condition.or(
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'SUCCESS'),
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'SUCCEEDED'),
        ),
        succeed,
      )
      .when(
        sfn.Condition.or(
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'FAILED'),
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'CANCELLED'),
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'CANCELLING'),
        ),
        failPass,
      )
      .otherwise(wait);
    const iterator = reconStart.next(wait.next(getJobRun.next(decide)));

    // DISTRIBUTED Map for the same reason as ingest: a full 717-table recon
    // run would blow the 25,000 parent-history-event limit via the per-table
    // poll loop. Each table runs as its own child STANDARD execution.
    // toleratedFailurePercentage is native; if left at the strict default 0,
    // recon still fails on the first bad table (matching ingest) — operators
    // wanting a resilient audit set it > 0 deliberately.
    const mapState = new sfn.DistributedMap(this, 'ReconPerTableMap', {
      itemsPath: '$.tables',
      maxConcurrency: props.reconMapConcurrency,
      mapExecutionType: sfn.StateMachineType.STANDARD,
      toleratedFailurePercentage: props.mapToleratedFailurePercentage || undefined,
      itemSelector: {
        'tableName.$':    '$$.Map.Item.Value.tableName',
      },
      resultPath: '$.mapResults',
    });
    mapState.itemProcessor(iterator);

    const notifyDone = new tasks.SnsPublish(this, 'NotifyReconCompletion', {
      topic: this.notificationTopic,
      subject: `${props.customerName} CDA Iceberg recon completed`,
      message: sfn.TaskInput.fromText('Tier D recon finished. Query cda_recon_results for findings.'),
      resultPath: sfn.JsonPath.DISCARD,
    }).next(new sfn.Succeed(this, 'ReconRunSucceeded'));

    const definition = listTables.next(mapState).next(notifyDone);

    const smLogs = new logs.LogGroup(this, 'ReconStateMachineLogs', {
      retention: snapRetention(props.logRetentionDays),
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const reconSm = new sfn.StateMachine(this, 'ReconStateMachine', {
      stateMachineName: `${props.customerName}-cda-iceberg-recon`,
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      role: smRole,
      logs: { destination: smLogs, level: sfn.LogLevel.ALL, includeExecutionData: true },
      tracingEnabled: true,
    });

    new events.Rule(this, 'ReconScheduleRule', {
      ruleName: `${props.customerName}-cda-iceberg-recon`,
      schedule: events.Schedule.expression(`cron(${props.reconCronExpression})`),
      enabled: props.reconScheduleEnabled,
      targets: [new eventsTargets.SfnStateMachine(reconSm)],
    });

    return reconSm;
  }

  /**
   * Build the spark-submit fragment that puts the Iceberg + S3 Tables runtime
   * on the job classpath. Returns the `--jars s3://.../deps/*` (default) or
   * `--packages <maven coords>` (escape hatch) tokens.
   *
   * WHY --jars by default: `--packages` resolves from Maven Central at EVERY
   * job start. Under Map concurrency, simultaneous jobs hammer Maven Central
   * and a fraction get transient "module not found" failures that kill the job
   * before any work runs (observed 2026-06-02: 16/700 recon jobs failed this
   * way). The two coordinates are shaded "runtime" uber-jars, so staging the
   * two files in S3 (data_load.sh ship_deps → gradle stageRuntimeDeps) and
   * referencing them with --jars is a complete, in-region, Maven-free replacement.
   *
   * The jar filenames are deterministic from the version props — they match
   * what `gradle stageRuntimeDeps` produces (Gradle names a resolved artifact
   * `<artifactId>-<version>.jar`).
   */
  private runtimeClasspathTokens(props: OrchestrationStackProps): string[] {
    if (props.useMavenPackages) {
      const sparkPackages = [
        `software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:${props.s3TablesCatalogVersion}`,
        `org.apache.iceberg:iceberg-spark-runtime-${props.sparkScalaVersion}:${props.icebergRuntimeVersion}`,
      ].join(',');
      return ['--packages', sparkPackages];
    }
    const depsPrefix = `s3://${props.bucketNames.artifact}/deps`;
    const jars = [
      `${depsPrefix}/iceberg-spark-runtime-${props.sparkScalaVersion}-${props.icebergRuntimeVersion}.jar`,
      `${depsPrefix}/s3-tables-catalog-for-iceberg-runtime-${props.s3TablesCatalogVersion}.jar`,
    ].join(',');
    return ['--jars', jars];
  }

  /** StartJobRun for recon — submits the same jar with `--recon-only`
    * args. Spark conf matches the medium ingest profile (200 GB disk +
    * 400 partitions); recon scans don't need the large-class headroom
    * because they're filtered to single-table SQL not full shuffles. */
  private makeReconStartJobRun(
    props: OrchestrationStackProps,
  ): tasks.CallAwsService {
    const c = SIZE_CONFS.medium;
    const sparkConfPairs = [
      'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
      'spark.sql.catalog.s3tables=org.apache.iceberg.spark.SparkCatalog',
      'spark.sql.catalog.s3tables.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog',
      `spark.sql.catalog.s3tables.warehouse=${props.icebergTableBucketArn}`,
      'spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED',
      'spark.executor.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED',
      `spark.driver.cores=${c.driverCores}`,
      `spark.driver.memory=${c.driverMemory}`,
      `spark.executor.cores=${c.executorCores}`,
      `spark.executor.memory=${c.executorMemory}`,
      `spark.emr-serverless.executor.disk=${c.executorDisk}`,
      `spark.sql.shuffle.partitions=${c.shufflePartitions}`,
    ];
    const sparkSubmitParams = [
      '--class', 'gw.cda.iceberg.IcebergIngest',
      ...this.runtimeClasspathTokens(props),
      ...sparkConfPairs.flatMap((kv) => ['--conf', kv]),
    ].join(' ');

    return new tasks.CallAwsService(this, 'StartReconJobRun', {
      service: 'emrserverless',
      action: 'startJobRun',
      parameters: {
        ApplicationId: props.emrServerlessApplicationId,
        ExecutionRoleArn: props.emrJobRoleArn,
        'ClientToken.$': 'States.UUID()',
        // tableName flows through unchanged; the EMR Serverless console
        // shows recon jobs as "<tableName>" same as ingest. We don't
        // prefix here because Step Functions doesn't allow combining
        // JsonPath with literals inside a single .$ string field for
        // CallAwsService parameters.
        'Name.$': '$.tableName',
        JobDriver: {
          SparkSubmit: {
            EntryPoint: `s3://${props.bucketNames.artifact}/jars/cda-iceberg-client-1.0.jar`,
            'EntryPointArguments.$':
              `States.Array('--recon-only', $.tableName, '${props.icebergNamespace}')`,
            SparkSubmitParameters: sparkSubmitParams,
          },
        },
        ConfigurationOverrides: {
          MonitoringConfiguration: {
            S3MonitoringConfiguration: {
              LogUri: `s3://${props.bucketNames.logs}/`,
            },
            // Also stream to CloudWatch so the RECON_WRITE_FAILED metric
            // filter + alarm can see driver logs (S3 logs aren't filterable).
            CloudWatchLoggingConfiguration: {
              Enabled: true,
              LogGroupName: this.emrLogGroupName,
            },
          },
        },
      },
      iamResources: [
        `arn:${this.partition}:emr-serverless:${this.region}:${this.account}:/applications/${props.emrServerlessApplicationId}`,
      ],
      iamAction: 'emr-serverless:StartJobRun',
      resultPath: '$.startResult',
    });
  }

  // ---------------------------------------------------------------------
  // Per-table iterator: Choice on $.sizeClass → one of three differently
  // sized StartJobRun states → shared wait/poll loop. Lets a 5B-row
  // transaction table get a 24 GB driver and 400 GB executor disk while
  // a 100-row typecode stays on default cores.
  // ---------------------------------------------------------------------
  private buildPerTableIterator(
    props: OrchestrationStackProps,
    sourceBucketName: string,
  ): sfn.IChainable {
    // Three branches, one per size class. Each is a fresh chain of
    // wait/poll/decide states (CDK's Choice doesn't share tails across
    // branches — each terminal needs unique state ids).
    const small  = this.makeStartJobRun(props, sourceBucketName, 'small')
      .next(this.makeTail(props, 'small'));
    const medium = this.makeStartJobRun(props, sourceBucketName, 'medium')
      .next(this.makeTail(props, 'medium'));
    const large  = this.makeStartJobRun(props, sourceBucketName, 'large')
      .next(this.makeTail(props, 'large'));

    return new sfn.Choice(this, 'TableSizeClass')
      .when(sfn.Condition.stringEquals('$.sizeClass', 'large'),  large)
      .when(sfn.Condition.stringEquals('$.sizeClass', 'medium'), medium)
      .otherwise(small);
  }

  /** Build a StartJobRun task whose spark conf matches the given size class. */
  private makeStartJobRun(
    props: OrchestrationStackProps,
    sourceBucketName: string,
    sizeClass: 'small' | 'medium' | 'large',
  ): tasks.CallAwsService {
    const c = SIZE_CONFS[sizeClass];
    const sparkConfPairs = [
      'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
      'spark.sql.catalog.s3tables=org.apache.iceberg.spark.SparkCatalog',
      'spark.sql.catalog.s3tables.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog',
      `spark.sql.catalog.s3tables.warehouse=${props.icebergTableBucketArn}`,
      'spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED',
      'spark.executor.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED',
      `spark.driver.cores=${c.driverCores}`,
      `spark.driver.memory=${c.driverMemory}`,
      `spark.executor.cores=${c.executorCores}`,
      `spark.executor.memory=${c.executorMemory}`,
      `spark.emr-serverless.executor.disk=${c.executorDisk}`,
      `spark.sql.shuffle.partitions=${c.shufflePartitions}`,
    ];

    // Column exclusion: pass the spec to the Spark job via a driver env
    // var. spark-submit splits SparkSubmitParameters on whitespace, so the
    // spec must contain no spaces (comma-separated, e.g.
    // "ssn,taxid,cc_claim:description"). CDK validates this in bin/app.ts.
    if (props.columnsToExclude) {
      sparkConfPairs.push(
        `spark.emr-serverless.driverEnv.COLUMNS_TO_EXCLUDE=${props.columnsToExclude}`,
      );
    }

    const sparkSubmitParams = [
      '--class', 'gw.cda.iceberg.IcebergIngest',
      ...this.runtimeClasspathTokens(props),
      ...sparkConfPairs.flatMap((kv) => ['--conf', kv]),
    ].join(' ');

    return new tasks.CallAwsService(this, `StartTableJobRun-${sizeClass}`, {
      service: 'emrserverless',
      action: 'startJobRun',
      parameters: {
        ApplicationId: props.emrServerlessApplicationId,
        ExecutionRoleArn: props.emrJobRoleArn,
        'ClientToken.$': 'States.UUID()',
        'Name.$': '$.tableName',
        JobDriver: {
          SparkSubmit: {
            EntryPoint: `s3://${props.bucketNames.artifact}/jars/cda-iceberg-client-1.0.jar`,
            'EntryPointArguments.$':
              `States.Array($.tableName, '${props.icebergNamespace}', ` +
              `'${sourceBucketName}', '${props.cdaManifestKey}', ` +
              `'${props.customerName}-cda-iceberg-state')`,
            SparkSubmitParameters: sparkSubmitParams,
          },
        },
        ConfigurationOverrides: {
          MonitoringConfiguration: {
            S3MonitoringConfiguration: {
              LogUri: `s3://${props.bucketNames.logs}/`,
            },
            // Also stream to CloudWatch so the RECON_WRITE_FAILED metric
            // filter + alarm can see driver logs (S3 logs aren't filterable).
            CloudWatchLoggingConfiguration: {
              Enabled: true,
              LogGroupName: this.emrLogGroupName,
            },
          },
        },
      },
      iamResources: [
        `arn:${this.partition}:emr-serverless:${this.region}:${this.account}:/applications/${props.emrServerlessApplicationId}`,
      ],
      iamAction: 'emr-serverless:StartJobRun',
      resultPath: '$.startResult',
    });
  }

  /** Build a wait/poll/decide tail for one size-class branch. CDK
   * requires each Choice branch be its own chain of unique state nodes,
   * so we emit a fresh tail per class with the size suffix in node ids. */
  private makeTail(props: OrchestrationStackProps, suffix: string): sfn.IChainable {
    const wait = new sfn.Wait(this, `WaitForTableJobRun-${suffix}`, {
      time: sfn.WaitTime.duration(Duration.seconds(props.serverlessPollIntervalSeconds)),
    });

    const getJobRun = new tasks.CallAwsService(this, `GetTableJobRun-${suffix}`, {
      service: 'emrserverless',
      action: 'getJobRun',
      parameters: {
        ApplicationId: props.emrServerlessApplicationId,
        JobRunId: sfn.JsonPath.stringAt('$.startResult.JobRunId'),
      },
      iamResources: [
        `arn:${this.partition}:emr-serverless:${this.region}:${this.account}:/applications/${props.emrServerlessApplicationId}/jobruns/*`,
      ],
      iamAction: 'emr-serverless:GetJobRun',
      resultPath: '$.jobRun',
    });

    const succeed = new sfn.Pass(this, `TableSucceeded-${suffix}`, {
      parameters: {
        'tableName.$': '$.tableName',
        'ts.$': '$.ts',
        'sizeClass.$': '$.sizeClass',
        'jobRunId.$': '$.startResult.JobRunId',
        state: 'SUCCESS',
      },
    });

    const fail = new sfn.Fail(this, `TableJobRunFailed-${suffix}`, {
      errorPath: sfn.JsonPath.format(
        'TableJobFailed:{}',
        sfn.JsonPath.stringAt('$.tableName'),
      ),
      causePath: sfn.JsonPath.stringAt('$.jobRun.JobRun.StateDetails'),
    });

    const decide = new sfn.Choice(this, `TableJobRunState-${suffix}`)
      .when(
        sfn.Condition.or(
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'SUCCESS'),
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'SUCCEEDED'),
        ),
        succeed,
      )
      .when(
        sfn.Condition.or(
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'FAILED'),
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'CANCELLED'),
          sfn.Condition.stringEquals('$.jobRun.JobRun.State', 'CANCELLING'),
        ),
        fail,
      )
      .otherwise(wait);

    return wait.next(getJobRun.next(decide));
  }
}

function arnToBucketName(arn: string): string {
  const m = arn.match(/^arn:[^:]+:s3:::([^/]+)$/);
  if (!m) throw new Error(`Not a valid S3 bucket ARN: ${arn}`);
  return m[1];
}
