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
    private static final int DEFAULT_MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES = 8;
    private static final int DEFAULT_DESCRIBE_STREAM_RETRY_TIMES = 50;
    private static final long DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS = 1000L;
    /**
     * If jitter is not enabled, the default combination of DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_BASE_MILLIS,
     * DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_MULTIPLIER_MILLIS and,
     * DEFAULT_MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES is such that retries for fixing
     * shard graph occur for a little more than 60 seconds. The sequence obtained for 8 retries starting from 0 and
     * ending at 7 is {1400, 1600, 2000, 2800, 4400, 7600, 14000, 26800}, the cumulative sum sequence, or total
     * duration sums for which is {1400, 3000, 5000, 7800, 12200, 19800, 33800, 60600}.
     */
    private static final boolean DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_JITTER_ENABLED = true;
    // Base for exponential back-off
    private static final long DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_BASE_MILLIS = 1200L;
    // Multiplier for exponential back-off
    private static final long DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_MULTIPLIER_MILLIS = 200L;

    // The LeaseManagementFactory interface doesnt have all the methods we need like the ones we pass while creating the ShardSyncTaskManager.
    // So we keep the type as DynamoDBLeaseManagementFactory.
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
        return new StreamsShardDetector(kinesisClient,
            streamName,
            DEFAULT_MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES,
            DEFAULT_DESCRIBE_STREAM_RETRY_TIMES,
            DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS,
            DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_JITTER_ENABLED,
            DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_BASE_MILLIS,
            DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_MULTIPLIER_MILLIS,
            sleeper,
            random);
    }
}
