package software.amazon.services.dynamodb.streamsadapter;

//import com.amazonaws.services.dynamodbv2.streamsadapter.model.AmazonServiceExceptionTransformer;
import software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.kinesis.paginators.ListStreamConsumersPublisher;
import software.amazon.services.dynamodb.streamsadapter.model.RequestResponseTranslator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class DynamoDBStreamsAsyncClientAdapter implements KinesisAsyncClient {

    private final DynamoDbStreamsAsyncClient internalClient;

    private static final Integer GET_RECORDS_LIMIT = 1000;

    private static final RequestResponseTranslator requestResponseTranslator = new RequestResponseTranslator();

    /**
     * Enum values decides the behavior of application when customer loses some records when KCL lags behind
     */
    public enum SkipRecordsBehavior {
        /**
         * Skips processing to the oldest available record
         */
        SKIP_RECORDS_TO_TRIM_HORIZON, /**
         * Throws an exception to KCL, which retries (infinitely) to fetch the data
         */
        KCL_RETRY;
    }

    private SkipRecordsBehavior skipRecordsBehavior = SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON;

    public DynamoDBStreamsAsyncClientAdapter(DynamoDbStreamsAsyncClient client) {
        internalClient = client;
    }

    @Override public String serviceName() {
        return internalClient.serviceName();
    }

    @Override public void close() {
        internalClient.close();
    }

    @Override
    public CompletableFuture<AddTagsToStreamResponse> addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<AddTagsToStreamResponse> addTagsToStream(Consumer<AddTagsToStreamRequest.Builder> addTagsToStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest createStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(Consumer<CreateStreamRequest.Builder> createStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DecreaseStreamRetentionPeriodResponse> decreaseStreamRetentionPeriod(
        DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DecreaseStreamRetentionPeriodResponse> decreaseStreamRetentionPeriod(Consumer<DecreaseStreamRetentionPeriodRequest.Builder> decreaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest deleteStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DeleteStreamResponse> deleteStream(Consumer<DeleteStreamRequest.Builder> deleteStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DeregisterStreamConsumerResponse> deregisterStreamConsumer(DeregisterStreamConsumerRequest deregisterStreamConsumerRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DeregisterStreamConsumerResponse> deregisterStreamConsumer(Consumer<DeregisterStreamConsumerRequest.Builder> deregisterStreamConsumerRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DescribeLimitsResponse> describeLimits(DescribeLimitsRequest describeLimitsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DescribeLimitsResponse> describeLimits(Consumer<DescribeLimitsRequest.Builder> describeLimitsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<DescribeLimitsResponse> describeLimits() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(DescribeStreamRequest describeStreamRequest) {
        CompletableFuture<software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse> responseFuture = internalClient.describeStream(requestResponseTranslator.translate(describeStreamRequest));
        software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse response = null;
        try {
            response = responseFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        if (response.streamDescription().streamStatus().equals("DISABLED")) {
            // FIXME short-term solution
            // KCL does not currently support the concept of disabled streams. If there
            // are no active shards (i.e. EndingSequenceNumber not null), then KCL will
            // not create leases and will not process any shards in that stream. As a
            // short-term solution, we feign active shards by setting the ending
            // sequence number of all leaf nodes in the shard tree to null.
            List<software.amazon.awssdk.services.dynamodb.model.Shard> allShards = getAllShardsForDisabledStream(response);
            List<software.amazon.awssdk.services.dynamodb.model.Shard> newList = markLeafShardsAsActive(allShards);
            software.amazon.awssdk.services.dynamodb.model.StreamDescription newStreamDescription =
                software.amazon.awssdk.services.dynamodb.model.StreamDescription.builder().shards(newList).lastEvaluatedShardId(null).creationRequestDateTime(response.streamDescription().creationRequestDateTime())
                    .keySchema(response.streamDescription().keySchema()).streamArn(response.streamDescription().streamArn())
                    .streamLabel(response.streamDescription().streamLabel()).streamStatus(response.streamDescription().streamStatus())
                    .tableName(response.streamDescription().tableName()).streamViewType(response.streamDescription().streamViewType()).build();
            response = software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse.builder().streamDescription(newStreamDescription).build();
        }
        return CompletableFuture.completedFuture(requestResponseTranslator.translate(response));
    }

    private List<software.amazon.awssdk.services.dynamodb.model.Shard> getAllShardsForDisabledStream(software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse initialResponse) {
        List<software.amazon.awssdk.services.dynamodb.model.Shard> shards =
            new ArrayList<>(initialResponse.streamDescription().shards());

        software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest request;
        software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse response = initialResponse;
        // Allowing KCL to paginate calls will not allow us to correctly determine the
        // leaf nodes. In order to avoid pagination issues when feigning shard activity, we collect all
        // shards in the adapter and return them at once.
        while (response.streamDescription().lastEvaluatedShardId() != null) {
            request = software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest.builder().streamArn(response.streamDescription().streamArn())
                .exclusiveStartShardId(response.streamDescription().lastEvaluatedShardId()).build();
            CompletableFuture<software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse> responseFuture = internalClient.describeStream(request);
            try {
                response = responseFuture.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            shards.addAll(response.streamDescription().shards());
        }
        return shards;
    }

    private List<software.amazon.awssdk.services.dynamodb.model.Shard> markLeafShardsAsActive(List<software.amazon.awssdk.services.dynamodb.model.Shard> shards) {
        List<String> parentShardIds = new ArrayList<String>();
        for (software.amazon.awssdk.services.dynamodb.model.Shard shard : shards) {
            if (shard.parentShardId() != null) {
                parentShardIds.add(shard.parentShardId());
            }
        }
        List<software.amazon.awssdk.services.dynamodb.model.Shard> newList = new ArrayList<>();

        for (software.amazon.awssdk.services.dynamodb.model.Shard shard : shards) {
            // Feign shard activity by modifying leaf shards
            if (!parentShardIds.contains(shard.shardId())) {
                software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange range = software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange.builder()
                    .startingSequenceNumber(shard.sequenceNumberRange().startingSequenceNumber())
                    .endingSequenceNumber(null)
                    .build();
                //shard.sequenceNumberRange(range);
                //shard.sequenceNumberRange().endingSequenceNumber(null);

                software.amazon.awssdk.services.dynamodb.model.Shard newShard = software.amazon.awssdk.services.dynamodb.model.Shard.builder()
                    .shardId(shard.shardId())
                    .parentShardId(shard.parentShardId())
                    .sequenceNumberRange(range)
                    .build();
                newList.add(newShard);
            } else {
                newList.add(shard);
            }
        }
        return newList;
    }

    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(Consumer<DescribeStreamRequest.Builder> describeStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DescribeStreamConsumerResponse> describeStreamConsumer(DescribeStreamConsumerRequest describeStreamConsumerRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DescribeStreamConsumerResponse> describeStreamConsumer(Consumer<DescribeStreamConsumerRequest.Builder> describeStreamConsumerRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DescribeStreamSummaryResponse> describeStreamSummary(DescribeStreamSummaryRequest describeStreamSummaryRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DescribeStreamSummaryResponse> describeStreamSummary(Consumer<DescribeStreamSummaryRequest.Builder> describeStreamSummaryRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DisableEnhancedMonitoringResponse> disableEnhancedMonitoring(
        DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DisableEnhancedMonitoringResponse> disableEnhancedMonitoring(Consumer<DisableEnhancedMonitoringRequest.Builder> disableEnhancedMonitoringRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<EnableEnhancedMonitoringResponse> enableEnhancedMonitoring(EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<EnableEnhancedMonitoringResponse> enableEnhancedMonitoring(Consumer<EnableEnhancedMonitoringRequest.Builder> enableEnhancedMonitoringRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<GetRecordsResponse> getRecords(GetRecordsRequest getRecordsRequest) {
        CompletableFuture<software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse> responseFuture = null;
        software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse response = null;

        if (getRecordsRequest.limit() != null && getRecordsRequest.limit() > GET_RECORDS_LIMIT) {
            responseFuture = internalClient.getRecords(requestResponseTranslator.translate(GetRecordsRequest.builder()
                .limit(GET_RECORDS_LIMIT)
                .shardIterator(getRecordsRequest.shardIterator())
                .build()
                ));
        }
        else {
            responseFuture = internalClient.getRecords(requestResponseTranslator.translate(getRecordsRequest));
        }

        //requestCache.addEntry(getRecordsRequest, requestAdapter);
        try {
            response = responseFuture.get();
            return CompletableFuture.completedFuture(requestResponseTranslator.translate(response));
        } catch (InterruptedException e) {
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<GetRecordsResponse> getRecords(Consumer<GetRecordsRequest.Builder> getRecordsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        CompletableFuture<software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse> responseFuture = internalClient.getShardIterator(requestResponseTranslator.translate(getShardIteratorRequest));
        software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse response = null;
        try {
            response = responseFuture.get();
            return CompletableFuture.completedFuture(requestResponseTranslator.translate(response));
        } catch (InterruptedException e) {
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        } catch (TrimmedDataAccessException e) {
            if (skipRecordsBehavior == DynamoDBStreamsAsyncClientAdapter.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                if (getShardIteratorRequest.shardIteratorTypeAsString().equals(ShardIteratorType.TRIM_HORIZON.toString())) {
                    //throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
                    e.printStackTrace();
                    return CompletableFuture.completedFuture(null);
                }
                //LOG.warn(String.format("Data has been trimmed. Intercepting DynamoDB exception and retrieving a fresh iterator %s", getShardIteratorRequest), e);
                return getShardIterator(GetShardIteratorRequest.builder()
                    .shardId(getShardIteratorRequest.shardId())
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .startingSequenceNumber(null)
                    .streamName(getShardIteratorRequest.streamName())
                    .timestamp(getShardIteratorRequest.timestamp())
                    .build());
            } else {
                //throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
                e.printStackTrace();
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(Consumer<GetShardIteratorRequest.Builder> getShardIteratorRequest) {
        return null;
    }

    @Override
    public CompletableFuture<IncreaseStreamRetentionPeriodResponse> increaseStreamRetentionPeriod(
        IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<IncreaseStreamRetentionPeriodResponse> increaseStreamRetentionPeriod(Consumer<IncreaseStreamRetentionPeriodRequest.Builder> increaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<ListShardsResponse> listShards(ListShardsRequest listShardsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListShardsResponse> listShards(Consumer<ListShardsRequest.Builder> listShardsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListStreamConsumersResponse> listStreamConsumers(ListStreamConsumersRequest listStreamConsumersRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListStreamConsumersResponse> listStreamConsumers(Consumer<ListStreamConsumersRequest.Builder> listStreamConsumersRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListStreamConsumersPublisher listStreamConsumersPaginator(ListStreamConsumersRequest listStreamConsumersRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListStreamConsumersPublisher listStreamConsumersPaginator(Consumer<ListStreamConsumersRequest.Builder> listStreamConsumersRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<ListStreamsResponse> listStreams(ListStreamsRequest listStreamsRequest) {
        CompletableFuture<software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse> responseFuture = internalClient.listStreams(requestResponseTranslator.translate(listStreamsRequest));
        software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse response = null;
        try {
            response = responseFuture.get();
            return CompletableFuture.completedFuture(requestResponseTranslator.translate(response));
        } catch (InterruptedException e) {
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<ListStreamsResponse> listStreams(Consumer<ListStreamsRequest.Builder> listStreamsRequest) {
        return null;
    }

    @Override public CompletableFuture<ListStreamsResponse> listStreams() {
        return null;
    }

    @Override
    public CompletableFuture<ListTagsForStreamResponse> listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListTagsForStreamResponse> listTagsForStream(Consumer<ListTagsForStreamRequest.Builder> listTagsForStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<MergeShardsResponse> mergeShards(MergeShardsRequest mergeShardsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<MergeShardsResponse> mergeShards(Consumer<MergeShardsRequest.Builder> mergeShardsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<PutRecordResponse> putRecord(PutRecordRequest putRecordRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<PutRecordResponse> putRecord(Consumer<PutRecordRequest.Builder> putRecordRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<PutRecordsResponse> putRecords(PutRecordsRequest putRecordsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<PutRecordsResponse> putRecords(Consumer<PutRecordsRequest.Builder> putRecordsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<RegisterStreamConsumerResponse> registerStreamConsumer(RegisterStreamConsumerRequest registerStreamConsumerRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<RegisterStreamConsumerResponse> registerStreamConsumer(Consumer<RegisterStreamConsumerRequest.Builder> registerStreamConsumerRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<RemoveTagsFromStreamResponse> removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<RemoveTagsFromStreamResponse> removeTagsFromStream(Consumer<RemoveTagsFromStreamRequest.Builder> removeTagsFromStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override public CompletableFuture<SplitShardResponse> splitShard(SplitShardRequest splitShardRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<SplitShardResponse> splitShard(Consumer<SplitShardRequest.Builder> splitShardRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<StartStreamEncryptionResponse> startStreamEncryption(StartStreamEncryptionRequest startStreamEncryptionRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<StartStreamEncryptionResponse> startStreamEncryption(Consumer<StartStreamEncryptionRequest.Builder> startStreamEncryptionRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<StopStreamEncryptionResponse> stopStreamEncryption(StopStreamEncryptionRequest stopStreamEncryptionRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<StopStreamEncryptionResponse> stopStreamEncryption(Consumer<StopStreamEncryptionRequest.Builder> stopStreamEncryptionRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(SubscribeToShardRequest subscribeToShardRequest,
        SubscribeToShardResponseHandler asyncResponseHandler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(Consumer<SubscribeToShardRequest.Builder> subscribeToShardRequest,
        SubscribeToShardResponseHandler asyncResponseHandler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<UpdateShardCountResponse> updateShardCount(UpdateShardCountRequest updateShardCountRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<UpdateShardCountResponse> updateShardCount(Consumer<UpdateShardCountRequest.Builder> updateShardCountRequest) {
        throw new UnsupportedOperationException();
    }
}
