import * as cdk from 'aws-cdk-lib';
import { aws_s3 as s3 } from 'aws-cdk-lib';
import { Construct } from 'constructs';

export class Service extends cdk.Stack {
  public readonly serviceBucket: s3.Bucket;
  public readonly launchServicesParameter: cdk.CfnParameter;

  constructor(scope: Construct, id: string, props: cdk.StackProps) {
    super(scope, id, props);

    this.serviceBucket = new s3.Bucket(this, 'Bucket', {
      bucketName: `wz-streaming-workshop-${this.account}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED
    });
  }
}
