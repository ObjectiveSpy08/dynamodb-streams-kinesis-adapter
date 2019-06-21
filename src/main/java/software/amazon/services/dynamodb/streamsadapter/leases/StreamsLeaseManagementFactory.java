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

import com.amazonaws.services.dynamodbv2.streamsadapter.utils.Sleeper;
import com.amazonaws.services.dynamodbv2.streamsadapter.utils.ThreadSleeper;
import lombok.Data;
import lombok.NonNull;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.leases.*;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseCoordinator;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.metrics.MetricsFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

@Data
public class StreamsLeaseManagementFactory implements LeaseManagementFactory{
    @NonNull
    private final KinesisAsyncClient kinesisClient;
    @NonNull
    private final String streamName;
    @NonNull
    private final DynamoDbAsyncClient dynamoDBClient;
    @NonNull
    private final String tableName;
    @NonNull
    private final String workerIdentifier;
    @NonNull
    private final ExecutorService executorService;
    @NonNull
    private final InitialPositionInStreamExtended initialPositionInStream;
    @NonNull
    private final HierarchicalShardSyncer hierarchicalShardSyncer;
    private Sleeper sleeper;
    private Random random;

    private final long failoverTimeMillis;
    private final long epsilonMillis;
    private final int maxLeasesForWorker;
    private final int maxLeasesToStealAtOneTime;
    private final int maxLeaseRenewalThreads;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean ignoreUnexpectedChildShards;
    private final long shardSyncIntervalMillis;
    private final boolean consistentReads;
    private final long listShardsBackoffTimeMillis;
    private final int maxListShardsRetryAttempts;
    private final int maxCacheMissesBeforeReload;
    private final long listShardsCacheAllowedAgeInSeconds;
    private final int cacheMissWarningModulus;
    private final long initialLeaseTableReadCapacity;
    private final long initialLeaseTableWriteCapacity;
    private final TableCreatorCallback tableCreatorCallback;
    private final Duration dynamoDbRequestTimeout;

    /**
     * Constructor.
     *
     * @param kinesisClient
     * @param streamName
     * @param dynamoDBClient
     * @param tableName
     * @param workerIdentifier
     * @param executorService
     * @param initialPositionInStream
     * @param failoverTimeMillis
     * @param epsilonMillis
     * @param maxLeasesForWorker
     * @param maxLeasesToStealAtOneTime
     * @param maxLeaseRenewalThreads
     * @param cleanupLeasesUponShardCompletion
     * @param ignoreUnexpectedChildShards
     * @param shardSyncIntervalMillis
     * @param consistentReads
     * @param listShardsBackoffTimeMillis
     * @param maxListShardsRetryAttempts
     * @param maxCacheMissesBeforeReload
     * @param listShardsCacheAllowedAgeInSeconds
     * @param cacheMissWarningModulus
     * @param initialLeaseTableReadCapacity
     * @param initialLeaseTableWriteCapacity
     * @param hierarchicalShardSyncer
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     */
    public StreamsLeaseManagementFactory(final KinesisAsyncClient kinesisClient, final String streamName,
        final DynamoDbAsyncClient dynamoDBClient, final String tableName, final String workerIdentifier,
        final ExecutorService executorService, final InitialPositionInStreamExtended initialPositionInStream,
        final long failoverTimeMillis, final long epsilonMillis, final int maxLeasesForWorker,
        final int maxLeasesToStealAtOneTime, final int maxLeaseRenewalThreads,
        final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
        final long shardSyncIntervalMillis, final boolean consistentReads, final long listShardsBackoffTimeMillis,
        final int maxListShardsRetryAttempts, final int maxCacheMissesBeforeReload,
        final long listShardsCacheAllowedAgeInSeconds, final int cacheMissWarningModulus,
        final long initialLeaseTableReadCapacity, final long initialLeaseTableWriteCapacity,
        final HierarchicalShardSyncer hierarchicalShardSyncer, final TableCreatorCallback tableCreatorCallback,
        Duration dynamoDbRequestTimeout) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.dynamoDBClient = dynamoDBClient;
        this.tableName = tableName;
        this.workerIdentifier = workerIdentifier;
        this.executorService = executorService;
        this.initialPositionInStream = initialPositionInStream;
        this.failoverTimeMillis = failoverTimeMillis;
        this.epsilonMillis = epsilonMillis;
        this.maxLeasesForWorker = maxLeasesForWorker;
        this.maxLeasesToStealAtOneTime = maxLeasesToStealAtOneTime;
        this.maxLeaseRenewalThreads = maxLeaseRenewalThreads;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardSyncIntervalMillis = shardSyncIntervalMillis;
        this.consistentReads = consistentReads;
        this.listShardsBackoffTimeMillis = listShardsBackoffTimeMillis;
        this.maxListShardsRetryAttempts = maxListShardsRetryAttempts;
        this.maxCacheMissesBeforeReload = maxCacheMissesBeforeReload;
        this.listShardsCacheAllowedAgeInSeconds = listShardsCacheAllowedAgeInSeconds;
        this.cacheMissWarningModulus = cacheMissWarningModulus;
        this.initialLeaseTableReadCapacity = initialLeaseTableReadCapacity;
        this.initialLeaseTableWriteCapacity = initialLeaseTableWriteCapacity;
        this.hierarchicalShardSyncer = hierarchicalShardSyncer;
        this.tableCreatorCallback = tableCreatorCallback;
        this.dynamoDbRequestTimeout = dynamoDbRequestTimeout;
    }

    @Override
    public LeaseCoordinator createLeaseCoordinator(@NonNull final MetricsFactory metricsFactory) {
        return new DynamoDBLeaseCoordinator(this.createLeaseRefresher(),
            workerIdentifier,
            failoverTimeMillis,
            epsilonMillis,
            maxLeasesForWorker,
            maxLeasesToStealAtOneTime,
            maxLeaseRenewalThreads,
            initialLeaseTableReadCapacity,
            initialLeaseTableWriteCapacity,
            metricsFactory);
    }

    @Override
    public ShardSyncTaskManager createShardSyncTaskManager(@NonNull final MetricsFactory metricsFactory) {
        return new ShardSyncTaskManager(this.createShardDetector(),
            this.createLeaseRefresher(),
            initialPositionInStream,
            cleanupLeasesUponShardCompletion,
            ignoreUnexpectedChildShards,
            shardSyncIntervalMillis,
            executorService,
            hierarchicalShardSyncer,
            metricsFactory);
    }

    @Override
    public DynamoDBLeaseRefresher createLeaseRefresher() {
        return new DynamoDBLeaseRefresher(tableName, dynamoDBClient, new DynamoDBLeaseSerializer(), consistentReads,
            tableCreatorCallback, dynamoDbRequestTimeout);
    }

    @Override
    public ShardDetector createShardDetector() {
        if (null == sleeper) {
            sleeper = new ThreadSleeper();
        }
        if (null == random) {
            random = ThreadLocalRandom.current();
        }
        return new StreamsShardDetector(kinesisClient, streamName, listShardsBackoffTimeMillis,
            maxListShardsRetryAttempts, listShardsCacheAllowedAgeInSeconds, maxCacheMissesBeforeReload,
            cacheMissWarningModulus, dynamoDbRequestTimeout, 8, 50, 1000L,
            true, 1200L, 200L, sleeper, random);
    }
}
