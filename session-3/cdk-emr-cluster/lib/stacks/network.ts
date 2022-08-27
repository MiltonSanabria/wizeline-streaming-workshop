import * as cdk from 'aws-cdk-lib';
import { aws_ec2 as ec2 } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { NetworkProps } from '../Interfaces/Inetwork';

export class Network extends cdk.Stack {
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: NetworkProps) {
    super(scope, id, props);

    this.vpc = new ec2.Vpc(this, 'Vpc', {
      cidr: props.cidr,
      maxAzs: props.maxAzs,
      subnetConfiguration: [
        {
          name: 'Public',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 28
        },
        {
          name: 'Private',
          subnetType: ec2.SubnetType.PRIVATE_WITH_NAT,
          cidrMask: 24
        }
      ],
      gatewayEndpoints: {
        S3: {
          service: ec2.GatewayVpcEndpointAwsService.S3,
          subnets: [{ subnetType: ec2.SubnetType.PRIVATE_WITH_NAT }]
        }
      }
    });
    this.vpc.addFlowLog('VpcFlowLogAll', {
      trafficType: ec2.FlowLogTrafficType.ALL
    });
  }
}
