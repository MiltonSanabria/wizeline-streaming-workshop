import * as cdk from 'aws-cdk-lib';

export interface NetworkProps extends cdk.StackProps {
  cidr: string;
  maxAzs: number;
}

export const devProps = {
  cidr: '10.0.0.0/16',
  maxAzs: 2
};
