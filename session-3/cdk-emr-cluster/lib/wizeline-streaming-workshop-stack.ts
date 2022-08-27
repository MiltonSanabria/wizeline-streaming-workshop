import * as stacks from './stacks';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
//wizeline-streaming-workshop/lib/stacks

export interface WizelineStreamingProps extends cdk.StageProps {
  vpcId: string;
}

export class WizelineStreamingWorkshopStack extends cdk.Stage {
  constructor(scope: Construct, id: string, props: WizelineStreamingProps) {
    super(scope, id, props);

    const { vpcId } =
      props || undefined;
    

    let network = new stacks.Network(this, 'Network', {
      cidr: '10.0.0.0/16',maxAzs: 2
    });

    let service = new stacks.Service(this, 'Service', {});
    // Build security stack.
    let securityStack = new stacks.Security(this, 'Security', {
      vpcId
    });
    
    new stacks.Emr(this, 'Emr', {
      vpcId,
      suffix: 'processing',
      bucket: service.serviceBucket,
      removeServiceParameters: false,
      orchestrator: securityStack.dataOrchestratorUser,
    });
  }
}
