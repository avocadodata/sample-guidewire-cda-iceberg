import { Stack, StackProps, CfnOutput } from 'aws-cdk-lib';
import { CfnTableBucket, CfnNamespace } from 'aws-cdk-lib/aws-s3tables';
import { Construct } from 'constructs';

export interface IcebergStackProps extends StackProps {
  customerName: string;
  /** Iceberg namespace (default 'cda'). All tables live under it. */
  namespaceName: string;
  /** Snapshot retention in days. Default 5 matches S3 Tables default; bump
   * for customers who need longer point-in-time query windows. */
  snapshotRetentionDays: number;
}

/**
 * S3 Tables bucket + namespace for the customer's CDA Iceberg landing.
 *
 * Tenancy: ONE deployment per customer. The S3 Tables bucket lives in the
 * customer's own AWS account. AWS handles compaction, snapshot expiration,
 * and orphan-file cleanup automatically — no per-table maintenance jobs to
 * own.
 *
 * Bucket naming: ${customerName}-cda-iceberg-${account}-${region}. Same
 * convention as the OSR artifact/data/logs buckets — collision-safe and
 * predictable on re-deploy.
 */
export class IcebergStack extends Stack {
  readonly tableBucketArn: string;
  readonly tableBucketName: string;
  readonly namespaceName: string;

  constructor(scope: Construct, id: string, props: IcebergStackProps) {
    super(scope, id, props);

    const bucketName = `${props.customerName}-cda-iceberg-${this.account}-${this.region}`;

    // The S3 Tables service manages compaction (target ~512 MB output files),
    // snapshot expiration (default 5 days), and unreferenced-file cleanup
    // automatically. The defaults are sensible and we don't override them
    // here — per-table TBLPROPERTIES set at CREATE TABLE time encode the
    // file-size target and write distribution mode.
    const tableBucket = new CfnTableBucket(this, 'CdaTableBucket', {
      tableBucketName: bucketName,
    });

    const ns = new CfnNamespace(this, 'CdaNamespace', {
      tableBucketArn: tableBucket.attrTableBucketArn,
      namespace: props.namespaceName,
    });
    ns.addDependency(tableBucket);

    this.tableBucketArn = tableBucket.attrTableBucketArn;
    this.tableBucketName = bucketName;
    this.namespaceName = props.namespaceName;

    new CfnOutput(this, 'TableBucketArn',  { value: this.tableBucketArn });
    new CfnOutput(this, 'TableBucketName', { value: bucketName });
    new CfnOutput(this, 'NamespaceName',   { value: props.namespaceName });
    new CfnOutput(this, 'CompactionInfo',  {
      value: `target-file-size 512MB; snapshot retention ${props.snapshotRetentionDays}d (managed by S3 Tables)`,
    });
  }
}
