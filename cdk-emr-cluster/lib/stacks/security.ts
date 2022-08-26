import {
    Stack, 
    StackProps,
} from 'aws-cdk-lib'

import * as iam from 'aws-cdk-lib/aws-iam';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { SecurityGroup } from 'aws-cdk-lib/aws-ec2';
import { CfnPrefixList } from 'aws-cdk-lib/aws-ec2';

import { Construct } from 'constructs';

export interface SecurityProps extends StackProps {
  vpcId: string;
}

export class Security extends Stack {

  public readonly dataOrchestratorUser: iam.User;

  public readonly securityGroups: { [name: string]: SecurityGroup };
  public readonly wizeIps: CfnPrefixList;
  constructor(scope: Construct, id: string, props: SecurityProps) {
    super(scope, id, props);

    const { vpcId } = props;
    // Lookup of existing vpc

    const vpc = ec2.Vpc.fromLookup(this, 'Vpc', {
      vpcId
    });


    this.dataOrchestratorUser = new iam.User(this, 'DataOrchestratorUser');
    let dataOrchestratorKeys = new iam.CfnAccessKey(
      this,
      'DataOrchestratorKeys',
      {
        userName: this.dataOrchestratorUser.userName
      }
    );
  }
}
