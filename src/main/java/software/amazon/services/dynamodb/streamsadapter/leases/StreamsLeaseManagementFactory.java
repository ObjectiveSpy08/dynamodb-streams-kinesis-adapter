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

package software.amazon.services.dynamodb.streamsadapter.leases;

import lombok.Data;
import lombok.NonNull;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementFactory;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseManagementFactory;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.services.dynamodb.streamsadapter.utils.Sleeper;
import software.amazon.services.dynamodb.streamsadapter.utils.ThreadSleeper;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@Data public class StreamsLeaseManagementFactory implements LeaseManagementFactory {
    // The LeaseManagementFactory interface doesnt have all the methods we need like the ones we pass while creating
    // the ShardSyncTaskManager.
    private DynamoDBLeaseManagementFactory internalFactory;
    private KinesisAsyncClient kinesisClient;
    private String streamName;
    private Sleeper sleeper;
    private Random random;

    /**
     * Constructor.
     *
     * @param leaseManagementFactory
     */
    public StreamsLeaseManagementFactory(final DynamoDBLeaseManagementFactory leaseManagementFactory,
        KinesisAsyncClient kinesisAsyncClient,
        String streamName) {
        this.internalFactory = leaseManagementFactory;
        this.kinesisClient = kinesisAsyncClient;
        this.streamName = streamName;
    }

    @Override public LeaseCoordinator createLeaseCoordinator(@NonNull final MetricsFactory metricsFactory) {
        return internalFactory.createLeaseCoordinator(metricsFactory);
    }

    @Override public ShardSyncTaskManager createShardSyncTaskManager(@NonNull final MetricsFactory metricsFactory) {
        return new ShardSyncTaskManager(this.createShardDetector(),
            this.createLeaseRefresher(),
            internalFactory.getInitialPositionInStream(),
            internalFactory.isCleanupLeasesUponShardCompletion(),
            internalFactory.isIgnoreUnexpectedChildShards(),
            internalFactory.getShardSyncIntervalMillis(),
            internalFactory.getExecutorService(),
            internalFactory.getHierarchicalShardSyncer(),
            metricsFactory);
    }

    @Override public DynamoDBLeaseRefresher createLeaseRefresher() {
        return internalFactory.createLeaseRefresher();
    }

    @Override public ShardDetector createShardDetector() {
        if (null == sleeper) {
            sleeper = new ThreadSleeper();
        }
        if (null == random) {
            random = ThreadLocalRandom.current();
        }
        return new StreamsShardDetector(kinesisClient, streamName, 8, 50, 1000L, true, 1200L, 200L, sleeper, random);
    }
}
