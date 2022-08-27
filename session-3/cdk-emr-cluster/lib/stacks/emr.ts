import {
    Stack, 
    StackProps,
    CfnParameter,
    CfnCondition,
    Fn
} from 'aws-cdk-lib'
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as emr from 'aws-cdk-lib/aws-emr';
import { Construct } from 'constructs';

export interface EmrProps extends StackProps{
    vpcId: string;
    suffix: string;
    bucket: s3.Bucket;
    orchestrator: iam.User;
    additionalSecurityGroups?: ec2.SecurityGroup[];
    removeServiceParameters?: boolean;
}

export class Emr extends Stack{
    public readonly emrCluster: emr.CfnCluster;

    constructor(scope: Construct, id: string, props: EmrProps) {
        super(scope, id, props);

        const {
            vpcId,
            suffix,
            bucket,
            orchestrator,
            removeServiceParameters,
            additionalSecurityGroups
            } = props || undefined;

        const vpc = ec2.Vpc.fromLookup(this, 'Vpc', {
            vpcName: 'StreamingWorkshop/Network/Vpc'
        });
        const additionalMasterSecurityGroups = additionalSecurityGroups
        ? additionalSecurityGroups.map((x) => {
            return x.securityGroupId;
            })
        : [];
        const additionalSlaveSecurityGroups = additionalSecurityGroups
        ? additionalSecurityGroups.map((x) => {
            return x.securityGroupId;
            })
        : [];

        const jobFlowRole = new iam.Role(this, 'JobFlowRole', {
        assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com')
        });
        
        jobFlowRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName(
            'service-role/AmazonElasticMapReduceforEC2Role'
        )
        );

        const jobFlowProfile = new iam.CfnInstanceProfile(this, 'JobFlowProfile', {
        roles: [jobFlowRole.roleName]
        });

        const serviceRole = new iam.Role(this, 'ServiceRole', {
        assumedBy: new iam.ServicePrincipal('elasticmapreduce.amazonaws.com')
        });

        serviceRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName(
            'service-role/AmazonElasticMapReduceRole'
        )
        );

        const autoScalingRole = new iam.Role(this, 'AutoScalingRole', {
        assumedBy: new iam.CompositePrincipal(
            new iam.ServicePrincipal('application-autoscaling.amazonaws.com'),
            new iam.ServicePrincipal('elasticmapreduce.amazonaws.com')
        )
        });

        autoScalingRole.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName(
            'service-role/AmazonElasticMapReduceforAutoScalingRole'
        ));
        this.emrCluster = new emr.CfnCluster(this, 'WorkshopEMRCluster', {
            name: `streaming-workshop-${suffix}`,
            releaseLabel: 'emr-6.5.0',

            // Logging
            logUri: bucket.s3UrlForObject('emr'),

            // Security
            visibleToAllUsers: false,
            jobFlowRole: jobFlowProfile.ref,
            serviceRole: serviceRole.roleName,
            autoScalingRole: autoScalingRole.roleName,

            // Specs
            stepConcurrencyLevel: 16,
            ebsRootVolumeSize: 64,
            configurations: [
                {
                classification: 'spark-hive-site',
                configurationProperties: {
                    'hive.metastore.client.factory.class':
                    'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                }
                }
            ],
            instances: {
                ec2KeyName: 'wz-streaming-workshop-kp',
                terminationProtected: false,
                ec2SubnetId: vpc.selectSubnets({
                subnetGroupName: 'Private'
                }).subnetIds[0],
                additionalMasterSecurityGroups,
                additionalSlaveSecurityGroups,
                masterInstanceGroup: {
                name: `Workshop-EMR-Master | ${suffix}`,
                instanceCount: 1,
                instanceType: 'm3.xlarge'
                },

                coreInstanceGroup: {
                name: `Workshop-EMR-Core | ${suffix}`,
                instanceCount: 1,
                instanceType: 'm3.xlarge',
                autoScalingPolicy: {
                    constraints: {
                    maxCapacity: 3,
                    minCapacity: 1
                    },
                    rules: [
                    {
                        name: 'EMRautoScaling',
                        description:
                        'Rule for the autoscaling group for the EMR cluster',
                        action: {
                        simpleScalingPolicyConfiguration: {
                            scalingAdjustment: 1,
                            adjustmentType: 'CHANGE_IN_CAPACITY',
                            coolDown: 300
                        }
                        },
                        trigger: {
                        cloudWatchAlarmDefinition: {
                            metricName: 'YARNMemoryAvailablePercentage',
                            comparisonOperator: 'LESS_THAN',
                            period: 300,
                            threshold: 15,
                            unit: 'PERCENT',
                            statistic: 'AVERAGE',
                            namespace: 'AWS/ElasticMapReduce'
                        }
                        }
                    },
                    {
                        name: 'EMR-scale-in',
                        description: 'Rule for scale in group for the EMR cluster',
                        action: {
                        simpleScalingPolicyConfiguration: {
                            scalingAdjustment: -1,
                            adjustmentType: 'CHANGE_IN_CAPACITY',
                            coolDown: 300
                        }
                        },
                        trigger: {
                        cloudWatchAlarmDefinition: {
                            metricName: 'YARNMemoryAvailablePercentage',
                            comparisonOperator: 'GREATER_THAN',
                            period: 300,
                            threshold: 80,
                            unit: 'PERCENT',
                            statistic: 'AVERAGE',
                            namespace: 'AWS/ElasticMapReduce'
                        }
                        }
                    }
                    ]
                }
                }
            },

            // Add-ons
            // TODO - Define requirements as bootstrap actions.
            bootstrapActions: [],
            applications: ['spark', 'zeppelin', 'hadoop'].map((v) => {
                return {
                name: v
                };
            })
        });

        if (!removeServiceParameters) {
            let launchService = new CfnParameter(this, 'LaunchService', {
              type: 'String',
              default: 'true',
              allowedValues: ['true', 'false']
            });
            this.emrCluster.cfnOptions.condition = new CfnCondition(
              this,
              'LaunchServiceCondition',
              {
                expression: Fn.conditionEquals(
                  launchService.valueAsString,
                  'true'
                )
              }
            );
        }
        new iam.ManagedPolicy(this, 'OrchestrationPolicy', {
            statements: [
              new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                  'elasticmapreduce:DescribeStep',
                  'elasticmapreduce:ListSteps',
                  'elasticmapreduce:AddJobFlowSteps',
                  'elasticmapreduce:DescribeCluster',
                  'elasticmapreduce:DescribeJobFlows',
                  'elasticmapreduce:TerminateJobFlows',
                  'elasticmapreduce:ListClusters',
                  'elasticmapreduce:RunJobFlow'
                ],
                resources: ['*']
              })
            ],
            users: [orchestrator]
        });
    }
}