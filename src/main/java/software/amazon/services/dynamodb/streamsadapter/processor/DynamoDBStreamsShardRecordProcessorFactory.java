package software.amazon.services.dynamodb.streamsadapter.processor;

import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public interface DynamoDBStreamsShardRecordProcessorFactory extends ShardRecordProcessorFactory {
    /**
     * Returns a new instance of the DynamoDBStreamsShardRecordProcessor
     *
     * @return An instance of DynamoDBStreamsShardRecordProcessor
     */
    DynamoDBStreamsShardRecordProcessor shardRecordProcessor();
}
