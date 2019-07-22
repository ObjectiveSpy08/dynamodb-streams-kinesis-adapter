package software.amazon.services.dynamodb.streamsadapter;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.utils.CompletableFutureUtils;
import software.amazon.services.dynamodb.streamsadapter.exceptions.ExceptionTranslator;
import software.amazon.services.dynamodb.streamsadapter.model.RequestResponseTranslator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DynamoDBStreamsAsyncClientAdapter implements KinesisAsyncClient {

    private static final Integer GET_RECORDS_LIMIT = 1000;
    private final RequestResponseTranslator requestResponseTranslator = new RequestResponseTranslator();
    private final ExceptionTranslator exceptionTranslator = new ExceptionTranslator();
    private SkipRecordsBehavior skipRecordsBehavior = SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON;
    private final DynamoDbStreamsAsyncClient internalClient;

    public DynamoDBStreamsAsyncClientAdapter(final DynamoDbStreamsAsyncClient client) {
        internalClient = client;
    }

    @Override public String serviceName() {
        return internalClient.serviceName();
    }

    @Override
    public void close() {
        internalClient.close();
    }

    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(DescribeStreamRequest describeStreamRequest) {
        software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse response;

        try {
            response = internalClient.describeStream(requestResponseTranslator.translate(describeStreamRequest)).get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            return CompletableFutureUtils.failedFuture(t instanceof AwsServiceException ? exceptionTranslator.translateDescribeStreamException((AwsServiceException) t) : t);
        }
        return CompletableFuture.completedFuture(requestResponseTranslator.translate(response));
    }

    @Override public CompletableFuture<GetRecordsResponse> getRecords(GetRecordsRequest getRecordsRequest) {
        CompletableFuture<software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse> responseFuture;
        software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse response;

        if (getRecordsRequest.limit() != null && getRecordsRequest.limit() > GET_RECORDS_LIMIT) {
            responseFuture =
                internalClient.getRecords(requestResponseTranslator.translate(getRecordsRequest.toBuilder()
                    .limit(GET_RECORDS_LIMIT)
                    .build()));
        } else {
            responseFuture = internalClient.getRecords(requestResponseTranslator.translate(getRecordsRequest));
        }

        try {
            response = responseFuture.get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            return CompletableFutureUtils.failedFuture(t instanceof AwsServiceException ? exceptionTranslator.translateGetRecordsException((AwsServiceException) t,
                skipRecordsBehavior) : t);
        }

        return CompletableFuture.completedFuture(requestResponseTranslator.translate(response));
    }

    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse response;
        try {
            response =
                internalClient.getShardIterator(requestResponseTranslator.translate(getShardIteratorRequest)).get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();

            if (t instanceof software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException
                && skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON
                && !getShardIteratorRequest.shardIteratorTypeAsString()
                .equals(ShardIteratorType.TRIM_HORIZON.toString())) {
                //LOG.warn(String.format("Data has been trimmed. Intercepting DynamoDB exception and retrieving a
                // fresh iterator %s", getShardIteratorRequest), e);

                return getShardIterator(getShardIteratorRequest.toBuilder()
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .startingSequenceNumber(null)
                    .build());
            }

            return CompletableFutureUtils.failedFuture(t instanceof AwsServiceException ? exceptionTranslator.translateGetShardIteratorException((AwsServiceException) t,
                skipRecordsBehavior) : t);
        }
        return CompletableFuture.completedFuture(requestResponseTranslator.translate(response));
    }

    @Override public CompletableFuture<ListStreamsResponse> listStreams(ListStreamsRequest listStreamsRequest) {
        software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse response;
        try {
            response = internalClient.listStreams(requestResponseTranslator.translate(listStreamsRequest)).get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            return CompletableFutureUtils.failedFuture(t instanceof AwsServiceException ? exceptionTranslator.translateListStreamsException((AwsServiceException) t) : t);
        }
        return CompletableFuture.completedFuture(requestResponseTranslator.translate(response));
    }

    /**
     * Enum values decides the behavior of application when customer loses some records when KCL lags behind
     */
    public enum SkipRecordsBehavior {
        /**
         * Skips processing to the oldest available record
         */
        SKIP_RECORDS_TO_TRIM_HORIZON,
        /**
         * Throws an exception to KCL, which retries (infinitely) to fetch the data
         */
        KCL_RETRY
    }
}
