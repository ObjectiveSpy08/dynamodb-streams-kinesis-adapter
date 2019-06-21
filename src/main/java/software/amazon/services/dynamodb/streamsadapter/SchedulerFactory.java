/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.services.dynamodb.streamsadapter;

import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.leases.LeaseManagementConfig;

import software.amazon.services.dynamodb.streamsadapter.leases.StreamsLeaseManagementFactory;

/*
 */
public class SchedulerFactory {
    private static StreamsLeaseManagementFactory createLeaseManagementFactory(LeaseManagementConfig leaseConfig) {
        return new StreamsLeaseManagementFactory(leaseConfig.kinesisClient(),
            leaseConfig.streamName(),
            leaseConfig.dynamoDBClient(),
            leaseConfig.tableName(),
            leaseConfig.workerIdentifier(),
            leaseConfig.executorService(),
            leaseConfig.initialPositionInStream(),
            leaseConfig.failoverTimeMillis(),
            leaseConfig.epsilonMillis(),
            leaseConfig.maxLeasesForWorker(),
            leaseConfig.maxLeasesToStealAtOneTime(),
            leaseConfig.maxLeaseRenewalThreads(),
            leaseConfig.cleanupLeasesUponShardCompletion(),
            leaseConfig.ignoreUnexpectedChildShards(),
            leaseConfig.shardSyncIntervalMillis(),
            leaseConfig.consistentReads(),
            leaseConfig.listShardsBackoffTimeInMillis(),
            leaseConfig.maxListShardsRetryAttempts(),
            leaseConfig.maxCacheMissesBeforeReload(),
            leaseConfig.listShardsCacheAllowedAgeInSeconds(),
            leaseConfig.cacheMissWarningModulus(),
            leaseConfig.initialLeaseTableReadCapacity(),
            leaseConfig.initialLeaseTableWriteCapacity(),
            leaseConfig.hierarchicalShardSyncer(),
            leaseConfig.tableCreatorCallback(),
            leaseConfig.dynamoDbRequestTimeout()
        );
    }

    public static Scheduler createDynamoDBStreamsScheduler(ConfigsBuilder configsBuilder) {
        LeaseManagementConfig leaseManagementConfig = configsBuilder.leaseManagementConfig();

        return new Scheduler(
            configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig().shardConsumerDispatchPollIntervalMillis(500),
            leaseManagementConfig.leaseManagementFactory(createLeaseManagementFactory(leaseManagementConfig)),
            configsBuilder.lifecycleConfig(),
            configsBuilder.metricsConfig(),
            configsBuilder.processorConfig(),
            configsBuilder.retrievalConfig().initialPositionInStreamExtended(InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON))
                .retrievalSpecificConfig(new PollingConfig(configsBuilder.streamName(), configsBuilder.kinesisClient()).maxRecords(1000))
        );
    }

    public static Scheduler createDynamoDBStreamsScheduler(CheckpointConfig checkpointConfig, CoordinatorConfig coordinatorConfig,
        LeaseManagementConfig leaseManagementConfig, LifecycleConfig lifecycleConfig, MetricsConfig metricsConfig,
        ProcessorConfig processorConfig, RetrievalConfig retrievalConfig) {

        return new Scheduler(
            checkpointConfig,
            coordinatorConfig,
            leaseManagementConfig.leaseManagementFactory(createLeaseManagementFactory(leaseManagementConfig)),
            lifecycleConfig,
            metricsConfig,
            processorConfig,
            retrievalConfig
                .retrievalSpecificConfig(new PollingConfig(leaseManagementConfig.streamName(), leaseManagementConfig.kinesisClient()))
        );
    }


}
