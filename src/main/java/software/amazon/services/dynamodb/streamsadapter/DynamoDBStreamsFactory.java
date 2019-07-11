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

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseManagementFactory;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import software.amazon.services.dynamodb.streamsadapter.leases.StreamsLeaseManagementFactory;
import software.amazon.services.dynamodb.streamsadapter.processor.DynamoDBStreamsShardRecordProcessorFactory;

/*
 */
public final class DynamoDBStreamsFactory {

    public static ConfigsBuilder createConfigsBuilder(final String streamName,
        final String applicationName,
        final KinesisAsyncClient kinesisClient,
        final DynamoDbAsyncClient dynamoDBClient,
        final CloudWatchAsyncClient cloudWatchClient,
        final String workerIdentifier,
        final DynamoDBStreamsShardRecordProcessorFactory shardRecordProcessorFactory) {

        return new ConfigsBuilder(streamName,
            applicationName,
            kinesisClient,
            dynamoDBClient,
            cloudWatchClient,
            workerIdentifier,
            shardRecordProcessorFactory);
    }

    public static Scheduler createScheduler(final ConfigsBuilder configsBuilder) {
        final LeaseManagementConfig leaseManagementConfig = configsBuilder.leaseManagementConfig();
        // Casting to DynamoDBLeaseManagementFactory is risky. If leaseManagementConfig.leaseManagementFactory() call were to change the actual object
        // it returns, we will run into a ClassCastException. However, KCL doesn't define some required methods(see createShardSyncTaskManager in StreamsLeaseManagementFactory) needed
        // while injecting our own ShardDetector, in the LeaseManagementFactory interface. The correct approach is to have KCL update that interface.
        return new Scheduler(configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig(),
            leaseManagementConfig.leaseManagementFactory(new StreamsLeaseManagementFactory((DynamoDBLeaseManagementFactory) leaseManagementConfig
                .leaseManagementFactory(), leaseManagementConfig.kinesisClient(), leaseManagementConfig.streamName())),
            configsBuilder.lifecycleConfig(),
            configsBuilder.metricsConfig(),
            configsBuilder.processorConfig(),
            configsBuilder.retrievalConfig()
                .retrievalSpecificConfig(new PollingConfig(configsBuilder.streamName(),
                    configsBuilder.kinesisClient())));
    }

    public static Scheduler createScheduler(final CheckpointConfig checkpointConfig,
        final CoordinatorConfig coordinatorConfig,
        final LeaseManagementConfig leaseManagementConfig,
        final LifecycleConfig lifecycleConfig,
        final MetricsConfig metricsConfig,
        final ProcessorConfig processorConfig,
        final RetrievalConfig retrievalConfig) {
        // Casting to DynamoDBLeaseManagementFactory is risky. If leaseManagementConfig.leaseManagementFactory() call were to change the actual object
        // it returns, we will run into a ClassCastException. However, KCL doesn't define some required methods(see createShardSyncTaskManager in StreamsLeaseManagementFactory) needed
        // while injecting our own ShardDetector, in the LeaseManagementFactory interface. The correct approach is to have KCL update that interface.
        return new Scheduler(checkpointConfig,
            coordinatorConfig,
            leaseManagementConfig.leaseManagementFactory(new StreamsLeaseManagementFactory((DynamoDBLeaseManagementFactory) leaseManagementConfig
                .leaseManagementFactory(), leaseManagementConfig.kinesisClient(), leaseManagementConfig.streamName())),
            lifecycleConfig,
            metricsConfig,
            processorConfig,
            retrievalConfig.retrievalSpecificConfig(new PollingConfig(leaseManagementConfig.streamName(),
                leaseManagementConfig.kinesisClient())));
    }
}
