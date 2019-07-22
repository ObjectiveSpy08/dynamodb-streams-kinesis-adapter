package software.amazon.services.dynamodb.streamsadapter.utils;

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClientBuilder;

/**
 * Utility to setup KinesisAsyncClient to be used with KCL.
 */
public class DynamoDBStreamsClientUtil {

    /**
     * Creates a client from a builder.
     *
     * @param clientBuilder
     * @return
     */
    public static DynamoDbStreamsAsyncClient createDynamoDbStreamsAsyncClient(DynamoDbStreamsAsyncClientBuilder clientBuilder) {
        return adjustKinesisClientBuilder(clientBuilder).build();
    }

    public static DynamoDbStreamsAsyncClientBuilder adjustKinesisClientBuilder(DynamoDbStreamsAsyncClientBuilder builder) {
        return builder.httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(Integer.MAX_VALUE));
    }
}
