package software.amazon.services.dynamodb.streamsadapter;

import org.junit.Before;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class DynamoDBStreamsAsyncClientAdapterTest {
    private DynamoDbStreamsAsyncClient mockClient;

    private DynamoDBStreamsAsyncClientAdapter adapterClient;

    @Before
    public void setUpTest() {
//        mockClient = mock(DynamoDbStreamsAsyncClient.class);
//        adapterClient = new DynamoDBStreamsAsyncClientAdapter(mockClient);
//        when(mockClient.describeStream(any(com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest.class))).thenReturn(DESCRIBE_STREAM_RESULT);
//        when(mockClient.getShardIterator(any(com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest.class)))
//            .thenReturn(new com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult());
//        when(mockClient.getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class)))
//            .thenReturn(new com.amazonaws.services.dynamodbv2.model.GetRecordsResult().withRecords(new java.util.ArrayList<com.amazonaws.services.dynamodbv2.model.Record>()));
//        when(mockClient.listStreams(any(com.amazonaws.services.dynamodbv2.model.ListStreamsRequest.class)))
//            .thenReturn(new com.amazonaws.services.dynamodbv2.model.ListStreamsResult());
    }
}
