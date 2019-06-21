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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.streamsadapter.utils.Sleeper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Synchronized;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.common.KinesisRequestsBuilder;
import software.amazon.kinesis.retrieval.AWSExceptionManager;

/**
 *
 */
@Slf4j
@Accessors(fluent = true)
@KinesisClientInternalApi
public class StreamsShardDetector implements ShardDetector {

    @NonNull
    private final KinesisAsyncClient kinesisClient;
    @NonNull
    private final String streamName;
    private final long listShardsBackoffTimeInMillis;
    private final int maxListShardsRetryAttempts;

    private final int maxRetriesToResolveInconsistencies;
    private final int maxDescribeStreamRetryAttempts;
    private final long describeStreamBackoffTimeInMillis;
    private final boolean isInconsistencyResolutionRetryBackoffJitterEnabled;
    private final long inconsistencyResolutionRetryBackoffMultiplierInMillis;
    private final long inconsistencyResolutionRetryBackoffBaseInMillis;
    private final Random random;
    private final Sleeper sleeper;

    private final long listShardsCacheAllowedAgeInSeconds;
    private final int maxCacheMissesBeforeReload;
    private final int cacheMissWarningModulus;
    private final Duration kinesisRequestTimeout;
    private ShardGraph shardGraph;

    private volatile Map<String, Shard> cachedShardMap = null;
    private volatile Instant lastCacheUpdateTime;
    @Getter(AccessLevel.PACKAGE)
    private AtomicInteger cacheMisses = new AtomicInteger(0);
    private final AtomicReference<List<Shard>> listOfShardsSinceLastGet = new AtomicReference<>();

    private static final long MAX_SHARD_COUNT_TO_TRIGGER_RETRIES = 1500L;

    public StreamsShardDetector(KinesisAsyncClient kinesisClient, String streamName, long listShardsBackoffTimeInMillis,
        int maxListShardsRetryAttempts, long listShardsCacheAllowedAgeInSeconds, int maxCacheMissesBeforeReload,
        int cacheMissWarningModulus, Duration kinesisRequestTimeout, int maxRetriesToResolveInconsistencies, int maxDescribeStreamRetryAttempts, long describeStreamBackoffTimeInMillis,
        boolean isDefaultInconsistencyResolutionRetryBackoffJitterEnabled, long inconsistencyResolutionRetryBackoffBaseInMillis,
        long inconsistencyResolutionRetryBackoffMultiplierInMillis, Sleeper sleeper, Random random) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.listShardsBackoffTimeInMillis = listShardsBackoffTimeInMillis;
        this.maxListShardsRetryAttempts = maxListShardsRetryAttempts;
        this.listShardsCacheAllowedAgeInSeconds = listShardsCacheAllowedAgeInSeconds;
        this.maxCacheMissesBeforeReload = maxCacheMissesBeforeReload;
        this.cacheMissWarningModulus = cacheMissWarningModulus;
        this.kinesisRequestTimeout = kinesisRequestTimeout;
        this.maxRetriesToResolveInconsistencies = maxRetriesToResolveInconsistencies;
        this.maxDescribeStreamRetryAttempts = maxDescribeStreamRetryAttempts;
        this.describeStreamBackoffTimeInMillis = describeStreamBackoffTimeInMillis;
        this.isInconsistencyResolutionRetryBackoffJitterEnabled
            = isDefaultInconsistencyResolutionRetryBackoffJitterEnabled;
        this.inconsistencyResolutionRetryBackoffBaseInMillis = inconsistencyResolutionRetryBackoffBaseInMillis;
        this.inconsistencyResolutionRetryBackoffMultiplierInMillis
            = inconsistencyResolutionRetryBackoffMultiplierInMillis;
        this.sleeper = sleeper;
        this.random = random;
    }

    @Override
    public Shard shard(@NonNull final String shardId) {
//        if (CollectionUtils.isNullOrEmpty(this.cachedShardMap)) {
//            synchronized (this) {
//                if (CollectionUtils.isNullOrEmpty(this.cachedShardMap)) {
//                    listShards();
//                }
//            }
//        }
//
//        Shard shard = cachedShardMap.get(shardId);
//
//        if (shard == null) {
//            if (cacheMisses.incrementAndGet() > maxCacheMissesBeforeReload || shouldRefreshCache()) {
//                synchronized (this) {
//                    shard = cachedShardMap.get(shardId);
//
//                    if (shard == null) {
//                        log.info("Too many shard map cache misses or cache is out of date -- forcing a refresh");
//                        listShards();
//                        shard = cachedShardMap.get(shardId);
//
//                        if (shard == null) {
//                            log.warn("Even after cache refresh shard '{}' wasn't found. This could indicate a bigger"
//                                + " problem.", shardId);
//                        }
//
//                        cacheMisses.set(0);
//                    } else {
//                        //
//                        // If the shardmap got updated, go ahead and set cache misses to 0
//                        //
//                        cacheMisses.set(0);
//                    }
//                }
//            }
//        }
//
//        if (shard == null) {
//            final String message = String.format("Cannot find the shard given the shardId %s. Cache misses: %s",
//                shardId, cacheMisses);
//            if (cacheMisses.get() % cacheMissWarningModulus == 0) {
//                log.warn(message);
//            } else {
//                log.debug(message);
//            }
//        }
//
//        return shard;
        if (this.listOfShardsSinceLastGet.get() == null) {
            //Update this.listOfShardsSinceLastGet as needed.
            this.listShards();
        }

        for (Shard shard : listOfShardsSinceLastGet.get()) {
            if (shard.shardId().equals(shardId))  {
                return shard;
            }
        }

        // LOG.warn("Cannot find the shard given the shardId " + shardId);
        return null;
    }

    public DescribeStreamResponse getStreamInfo(String startShardId)
        throws ResourceNotFoundException, LimitExceededException {
        final DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
            .streamName(streamName)
            .exclusiveStartShardId(startShardId)
            .build();
        DescribeStreamResponse response = null;

        LimitExceededException lastException = null;

        int remainingRetryTimes = this.maxDescribeStreamRetryAttempts;
        // Call DescribeStream, with backoff and retries (if we get LimitExceededException).
        while (response == null) {
            try {
                response = kinesisClient.describeStream(describeStreamRequest).get();
            } catch (LimitExceededException le) {
                //LOG.info("Got LimitExceededException when describing stream " + streamName + ". Backing off for " + this.describeStreamBackoffTimeInMillis + " millis.");
                sleeper.sleep(this.describeStreamBackoffTimeInMillis);
                lastException = le;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            remainingRetryTimes--;
            if (remainingRetryTimes == 0 && response == null) {
                if (lastException != null) {
                    throw lastException;
                }
                throw new IllegalStateException("Received null from DescribeStream call.");
            }
        }

        final String streamStatus = response.streamDescription().streamStatusAsString();
        if (StreamStatus.ACTIVE.toString().equals(streamStatus)
            || StreamStatus.UPDATING.toString().equals(streamStatus)) {
            return response;
        } else {
            //LOG.info("Stream is in status " + streamStatus + ", DescribeStream returning null (wait until stream is Active or Updating");
            return null;
        }
    }

    @Override
    @Synchronized
    public List<Shard> listShards() {
        if (shardGraph == null) {
            shardGraph = new ShardGraph();
        }

        // ShardGraph may not be empty if this call is being made after DescribeStream throttling.
        // In that case, the graph will have a lot of closed leaf nodes since their descendants were not
        // discovered earlier due to throttling. We do not handle that explicitly and allow the next round of
        // inconsistency fix attempts to resolve it.
        if (buildShardGraphSnapshot() == ShardGraphProcessingResult.STREAM_DISABLED) {
            //LOG.info("Stream was disabled during getShardList operation.");
            return null;
        }

        if (shardGraph.size() < MAX_SHARD_COUNT_TO_TRIGGER_RETRIES) {
            int retryAttempt = 0;
            while (shardGraph.closedLeafNodeCount() > 0 && retryAttempt < maxRetriesToResolveInconsistencies) {
                final long backOffTime = getInconsistencyBackoffTimeInMillis(retryAttempt);
                //String infoMsg = String.format("Inconsistency resolution retry attempt: %d. Backing off for %d millis.", retryAttempt, backOffTime);
                //LOG.info(infoMsg);
                sleeper.sleep(backOffTime);
                ShardGraphProcessingResult shardGraphProcessingResult = resolveInconsistenciesInShardGraph();
                if (shardGraphProcessingResult.equals(ShardGraphProcessingResult.STREAM_DISABLED)) {
                    //LOG.info("Stream was disabled during getShardList operation.");
                    return null;
                } else if (shardGraphProcessingResult.equals(ShardGraphProcessingResult.RESOLVED_INCONSISTENCIES_AND_ABORTED)) {
                    //infoMsg = String.format("An intermediate page in DescribeStream response resolved inconsistencies. " + "Total retry attempts taken to resolve inconsistencies: %d", retryAttempt + 1);
                    //LOG.info(infoMsg);
                    break;
                }
                retryAttempt++;
            }
            if (retryAttempt == maxRetriesToResolveInconsistencies && shardGraph.closedLeafNodeCount() > 0) {
                // LOG.warn("Inconsistencies in the shard graph were not resolved after exhausting all retries.");
            }
        } else {
            if (shardGraph.closedLeafNodeCount() > 0) {
                String msg = String.format("Returning shard list with %s closed leaf node shards.",
                    shardGraph.closedLeafNodeCount());
                // LOG.debug(msg);
            }
        }

        this.listOfShardsSinceLastGet.set(shardGraph.getShards());
        this.shardGraph = new ShardGraph();
        return listOfShardsSinceLastGet.get();
    }

    private ShardGraphProcessingResult buildShardGraphSnapshot() {

        DescribeStreamResponse response;

        do {
            response = getStreamInfo(shardGraph.getLastFetchedShardId());
            if (response == null) {
                /*
                 * If getStreamInfo ever returns null, we should bail and return null from getShardList.
                 * This indicates the stream is not in ACTIVE state and we may not have accurate/consistent information
                 * about the stream. By returning ShardGraphProcessingResult.STREAM_DISABLED from here, we indicate that
                 * getStreamInfo returned a null response and the caller (getShardList) should return null. If, on the
                 * other hand, an exception is thrown from getStreamInfo, it will bubble up to the caller of
                 * getShardList, which then handles it accordingly.
                 */
                return ShardGraphProcessingResult.STREAM_DISABLED;
            } else {
                shardGraph.addNodes(response.streamDescription().shards());
                //LOG.debug(String.format("Building shard graph snapshot; total shard count: %d", shardGraph.size()));
            }
        } while (response.streamDescription().hasMoreShards());
        return ShardGraphProcessingResult.FETCHED_ALL_AVAILABLE_SHARDS;
    }

    private ShardGraphProcessingResult resolveInconsistenciesInShardGraph() {
        DescribeStreamResponse response;
        final String warnMsg = String.format("Inconsistent shard graph state detected. "
            + "Fetched: %d shards. Closed leaves: %d shards", shardGraph.size(), shardGraph.closedLeafNodeCount());
        //LOG.warn(warnMsg);
        /*if (LOG.isDebugEnabled()) {
            final String debugMsg = String.format("Following leaf node shards are closed: %s",
                String.join(", ", shardGraph.getAllClosedLeafNodeIds()));
            LOG.debug(debugMsg);
        }*/
        String exclusiveStartShardId = shardGraph.getEarliestClosedLeafNodeId();
        do {
            response = getStreamInfo(exclusiveStartShardId);
            if (response == null) {
                return ShardGraphProcessingResult.STREAM_DISABLED;
            } else {
                shardGraph.addToClosedLeafNodes(response.streamDescription().shards());
                //LOG.debug(String.format("Resolving inconsistencies in shard graph; total shard count: %d", shardGraph.size()));
                if (shardGraph.closedLeafNodeCount() == 0) {
                    return ShardGraphProcessingResult.RESOLVED_INCONSISTENCIES_AND_ABORTED;
                }
                exclusiveStartShardId = shardGraph.getLastFetchedShardId();
            }
        } while (response.streamDescription().hasMoreShards());
        return ShardGraphProcessingResult.FETCHED_ALL_AVAILABLE_SHARDS;
    }

    @VisibleForTesting
    long getInconsistencyBackoffTimeInMillis(int retryAttempt) {
        double baseMultiplier = isInconsistencyResolutionRetryBackoffJitterEnabled ? random.nextDouble() : 1.0;
        return (long)(baseMultiplier * inconsistencyResolutionRetryBackoffBaseInMillis) +
            (long)Math.pow(2.0, retryAttempt) * inconsistencyResolutionRetryBackoffMultiplierInMillis;
    }

    private ListShardsResponse listShards(final String nextToken) {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(LimitExceededException.class, t -> t);
        exceptionManager.add(ResourceInUseException.class, t -> t);
        exceptionManager.add(KinesisException.class, t -> t);

        ListShardsRequest.Builder request = KinesisRequestsBuilder.listShardsRequestBuilder();
        if (StringUtils.isEmpty(nextToken)) {
            request = request.streamName(streamName);
        } else {
            request = request.nextToken(nextToken);
        }
        ListShardsResponse result = null;
        LimitExceededException lastException = null;
        int remainingRetries = maxListShardsRetryAttempts;

        while (result == null) {

            try {
                try {
                    result = FutureUtils.resolveOrCancelFuture(kinesisClient.listShards(request.build()), kinesisRequestTimeout);
                } catch (ExecutionException e) {
                    throw exceptionManager.apply(e.getCause());
                } catch (InterruptedException e) {
                    // TODO: check if this is the correct behavior for Interrupted Exception
                    log.debug("Interrupted exception caught, shutdown initiated, returning null");
                    return null;
                }
            } catch (ResourceInUseException e) {
                log.info("Stream is not in Active/Updating status, returning null (wait until stream is in"
                    + " Active or Updating)");
                return null;
            } catch (LimitExceededException e) {
                log.info("Got LimitExceededException when listing shards {}. Backing off for {} millis.", streamName,
                    listShardsBackoffTimeInMillis);
                try {
                    Thread.sleep(listShardsBackoffTimeInMillis);
                } catch (InterruptedException ie) {
                    log.debug("Stream {} : Sleep  was interrupted ", streamName, ie);
                }
                lastException = e;
            } catch (TimeoutException te) {
                throw new RuntimeException(te);
            }
            remainingRetries--;
            if (remainingRetries <= 0 && result == null) {
                if (lastException != null) {
                    throw lastException;
                }
                throw new IllegalStateException("Received null from ListShards call.");
            }
        }
        return result;
    }

//    void cachedShardMap(final List<Shard> shards) {
//        cachedShardMap = shards.stream().collect(Collectors.toMap(Shard::shardId, Function.identity()));
//        lastCacheUpdateTime = Instant.now();
//    }

//    private boolean shouldRefreshCache() {
//        final Duration secondsSinceLastUpdate = Duration.between(lastCacheUpdateTime, Instant.now());
//        final String message = String.format("Shard map cache is %d seconds old", secondsSinceLastUpdate.getSeconds());
//        if (secondsSinceLastUpdate.compareTo(Duration.of(listShardsCacheAllowedAgeInSeconds, ChronoUnit.SECONDS)) > 0) {
//            log.info("{}. Age exceeds limit of {} seconds -- Refreshing.", message, listShardsCacheAllowedAgeInSeconds);
//            return true;
//        }
//
//        log.debug("{}. Age doesn't exceed limit of {} seconds.", message, listShardsCacheAllowedAgeInSeconds);
//        return false;
//    }

    private enum ShardGraphProcessingResult {
        STREAM_DISABLED,
        FETCHED_ALL_AVAILABLE_SHARDS,
        RESOLVED_INCONSISTENCIES_AND_ABORTED
    }

    private static class ShardNode {

        private final Shard shard;

        private final Set<String> descendants;

        ShardNode(Shard shard) {
            this.shard = shard;
            descendants = new HashSet<>();
        }

        public String getShardId() {
            return shard.shardId();
        }

        public Shard getShard() {
            return shard;
        }

        boolean isShardClosed() {
            return shard.sequenceNumberRange() != null &&
                shard.sequenceNumberRange().endingSequenceNumber() != null;
        }

        boolean addDescendant(String shardId) {
            return descendants.add(shardId);
        }
    }

    private static class ShardGraph {

        private final Map<String, ShardNode> nodes;

        private final TreeSet<String> closedLeafNodeIds;

        private String lastFetchedShardId;

        public ShardGraph() {
            nodes = new HashMap<>();
            closedLeafNodeIds = new TreeSet<>();
        }

        String getLastFetchedShardId() {
            return lastFetchedShardId;
        }

        String getEarliestClosedLeafNodeId() {
            if (closedLeafNodeIds.isEmpty()) {
                return null;
            } else {
                return closedLeafNodeIds.first();
            }
        }

        /**
         * Adds a list of shards to the graph.
         * @param shards List of shards to be added to the graph.
         */
        private void addNodes(List<Shard> shards) {
            if (null == shards) {
                return;
            }
//            if (LOG.isDebugEnabled()) {
//                LOG.debug(String.format("Updating the graph with the following shards: \n %s",
//                    String.join(", ", shards.stream().map(Shard::getShardId).collect(Collectors.toList()))));
//            }
            for (Shard shard : shards) {
                addNode(shard);
            }
            updateLastFetchedShardId(shards);
        }

        /**
         * Adds descendants only to closed leaf nodes in order to ensure all leaf nodes in
         * the graph are open.
         * @param shards list of shards obtained from DescribeStream call.
         */
        private void addToClosedLeafNodes(List<Shard> shards) {
            if (null == shards) {
                return;
            }
//            if (LOG.isDebugEnabled()) {
//                LOG.debug(String.format("Attempting to resolve inconsistencies in the graph with the following shards: \n %s",
//                    String.join(", ", shards.stream().map(Shard::getShardId).collect(Collectors.toList()))));
//            }
            for (Shard shard : shards) {
                final String parentShardId = shard.parentShardId();
                if (null != parentShardId && closedLeafNodeIds.contains(parentShardId)) {
                    ShardNode shardNode = addNode(shard);
                    closedLeafNodeIds.remove(parentShardId);
                    if (shardNode.isShardClosed()) {
                        closedLeafNodeIds.add(shardNode.getShardId());
                    }
                }
            }
            updateLastFetchedShardId(shards);
        }

        private void updateLastFetchedShardId(List<Shard> shards) {
            if (shards.size() > 0) {
                Shard lastShard = shards.get(shards.size() - 1);
                lastFetchedShardId = lastShard.shardId();
            }
        }

        private ShardNode addNode(Shard shard) {
            final ShardNode shardNode = new ShardNode(shard);
            nodes.put(shardNode.getShardId(), shardNode);
            // if the node is closed, add it to the closed leaf node set.
            // once its child appears, this node will be removed from the set.
            if (shardNode.isShardClosed()) {
                closedLeafNodeIds.add(shardNode.getShardId());
            }
            final String parentShardID = shard.parentShardId();
            // Ensure nodes contains the parent shard, since older shards are trimmed and we will see nodes whose
            // parent shards are not in the graph.
            if (null != parentShardID && nodes.containsKey(parentShardID)) {
                final ShardNode parentNode = nodes.get(parentShardID);
                parentNode.addDescendant(shard.shardId());
                closedLeafNodeIds.remove(parentShardID);
            }
            return shardNode;
        }

        private int size() {
            return nodes.size();
        }

        private int closedLeafNodeCount() {
            return closedLeafNodeIds.size();
        }

        Set<String> getAllClosedLeafNodeIds() {
            return closedLeafNodeIds;
        }

        List<Shard> getShards() {
            return nodes.values().stream().map(ShardNode::getShard).collect(Collectors.toList());
        }
    }
}
