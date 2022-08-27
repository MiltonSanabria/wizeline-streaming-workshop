#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import * as constants from '../lib/utils/constants';

import { WizelineStreamingWorkshopStack } from '../lib/wizeline-streaming-workshop-stack';

const app = new cdk.App();

const env_us  = { account: '<youraccount>', region: '<yourregion>' };

new WizelineStreamingWorkshopStack(app, `${constants.CDK_APP_NAME}`, {
  env: { account: '<youraccount>', region: '<yourregion>' },
  vpcId: '<yourvpc>'
});
