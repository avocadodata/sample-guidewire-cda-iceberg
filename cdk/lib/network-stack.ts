import { Stack, StackProps, CfnOutput, Tags, RemovalPolicy } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';

export interface NetworkStackProps extends StackProps {
  customerName: string;
  /** If empty, a brand-new VPC is provisioned. If set, the VPC is looked up. */
  existingVpcId: string;
  /** Comma-separated subnet IDs for compute (EMR Serverless). Required when
   * bringing your own VPC and you want to pin EMR to specific subnets. */
  existingPrivateSubnetIds: string;
  newVpcCidr: string;
}

/**
 * Network for the Iceberg ingest path: just enough VPC for EMR Serverless
 * to reach S3 (CDA source + S3 Tables warehouse) and any AWS service APIs
 * (Glue catalog, Athena if used downstream).
 *
 * Compared to the OSR network stack, there's no `databaseSubnets` here —
 * the Iceberg path doesn't provision RDS. If a customer also wants the
 * RDS hydration add-on, that ships in a separate (optional) stack and
 * brings its own database subnets.
 *
 * Two modes:
 *   1. BYO VPC: pass existingVpcId + (optional) existingPrivateSubnetIds.
 *   2. New VPC: a 2-tier VPC (PUBLIC / PRIVATE_WITH_EGRESS) in newVpcCidr,
 *      plus an S3 gateway endpoint so EMR can reach S3 without NAT egress.
 */
export class NetworkStack extends Stack {
  readonly vpc: ec2.IVpc;
  readonly computeSubnets: ec2.SubnetSelection;

  constructor(scope: Construct, id: string, props: NetworkStackProps) {
    super(scope, id, props);

    if (props.existingVpcId) {
      this.vpc = ec2.Vpc.fromLookup(this, 'ImportedVpc', {
        vpcId: props.existingVpcId,
      });

      const privateIds = props.existingPrivateSubnetIds
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);

      this.computeSubnets = privateIds.length > 0
        ? { subnets: privateIds.map((sid, i) => ec2.Subnet.fromSubnetId(this, `ImpComputeSubnet${i}`, sid)) }
        : { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS };
    } else {
      const vpc = new ec2.Vpc(this, 'CdaVpc', {
        ipAddresses: ec2.IpAddresses.cidr(props.newVpcCidr),
        maxAzs: 2,
        natGateways: 1,
        subnetConfiguration: [
          { name: 'public',  subnetType: ec2.SubnetType.PUBLIC,              cidrMask: 24 },
          { name: 'compute', subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS, cidrMask: 22 },
        ],
      });

      // VPC Flow Logs → CloudWatch Logs. Captures network flow records for
      // the EMR compute subnets so operators (and AppSec) can audit egress
      // and diagnose connectivity to the CDA source / S3 Tables endpoints.
      // cdk-nag AwsSolutions-VPC7. Retention is bounded to keep cost down;
      // bump if your compliance regime requires longer flow-log retention.
      vpc.addFlowLog('FlowLog', {
        destination: ec2.FlowLogDestination.toCloudWatchLogs(
          new logs.LogGroup(this, 'VpcFlowLogs', {
            logGroupName: `/aws/vpc/${props.customerName}-cda-iceberg-flowlogs`,
            retention: logs.RetentionDays.ONE_MONTH,
            removalPolicy: RemovalPolicy.DESTROY,
          }),
        ),
        trafficType: ec2.FlowLogTrafficType.ALL,
      });

      // S3 gateway endpoint avoids NAT egress for parquet reads from the
      // CDA source bucket and writes to the S3 Tables warehouse.
      vpc.addGatewayEndpoint('S3Endpoint', {
        service: ec2.GatewayVpcEndpointAwsService.S3,
        subnets: [{ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS }],
      });

      Tags.of(vpc).add('cda:customer', props.customerName);

      this.vpc = vpc;
      this.computeSubnets = { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS };
    }

    new CfnOutput(this, 'VpcId', { value: this.vpc.vpcId });
  }
}
