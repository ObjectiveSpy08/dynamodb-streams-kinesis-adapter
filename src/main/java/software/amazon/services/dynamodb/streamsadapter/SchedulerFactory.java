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
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.leases.LeaseManagementFactory;
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

    public static Scheduler createDynamoDBStreamsScheduler(ConfigsBuilder configsBuilder) {
        LeaseManagementConfig leaseManagementConfig = configsBuilder.leaseManagementConfig();

        return new Scheduler(
            configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig(),
            leaseManagementConfig.leaseManagementFactory(new StreamsLeaseManagementFactory(leaseManagementConfig.leaseManagementFactory(),
                    leaseManagementConfig.kinesisClient(), leaseManagementConfig.streamName())),
            configsBuilder.lifecycleConfig(),
            configsBuilder.metricsConfig(),
            configsBuilder.processorConfig(),
            configsBuilder.retrievalConfig()
                .retrievalSpecificConfig(new PollingConfig(configsBuilder.streamName(), configsBuilder.kinesisClient()))
        );
    }

    public static Scheduler createDynamoDBStreamsScheduler(CheckpointConfig checkpointConfig, CoordinatorConfig coordinatorConfig,
        LeaseManagementConfig leaseManagementConfig, LifecycleConfig lifecycleConfig, MetricsConfig metricsConfig,
        ProcessorConfig processorConfig, RetrievalConfig retrievalConfig) {

        return new Scheduler(
            checkpointConfig,
            coordinatorConfig,
            leaseManagementConfig.leaseManagementFactory(new StreamsLeaseManagementFactory(leaseManagementConfig.leaseManagementFactory(),
                leaseManagementConfig.kinesisClient(), leaseManagementConfig.streamName())),
            lifecycleConfig,
            metricsConfig,
            processorConfig,
            retrievalConfig
                .retrievalSpecificConfig(new PollingConfig(leaseManagementConfig.streamName(), leaseManagementConfig.kinesisClient()))
        );
    }
}
