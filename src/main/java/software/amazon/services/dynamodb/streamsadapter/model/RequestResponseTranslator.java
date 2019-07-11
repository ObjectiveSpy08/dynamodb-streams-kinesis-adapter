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

package software.amazon.services.dynamodb.streamsadapter.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

// TODO: No static methods. immutable map

public class RequestResponseTranslator {
    private static final Map<software.amazon.awssdk.services.kinesis.model.ShardIteratorType, ShardIteratorType>
        shardIteratorTypeMap =
        new EnumMap<>(software.amazon.awssdk.services.kinesis.model.ShardIteratorType.class);

    private static final Map<StreamStatus, software.amazon.awssdk.services.kinesis.model.StreamStatus>
        streamStatusMap =
        new EnumMap<>(StreamStatus.class);

    private static final ObjectMapper MAPPER = new RecordObjectMapper(new JavaTimeModule());

    static {
        shardIteratorTypeMap.put(software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER,
            ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        shardIteratorTypeMap.put(software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER,
            ShardIteratorType.AT_SEQUENCE_NUMBER);
        shardIteratorTypeMap.put(software.amazon.awssdk.services.kinesis.model.ShardIteratorType.LATEST,
            ShardIteratorType.LATEST);
        shardIteratorTypeMap.put(software.amazon.awssdk.services.kinesis.model.ShardIteratorType.TRIM_HORIZON,
            ShardIteratorType.TRIM_HORIZON);
        shardIteratorTypeMap.put(software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP,
            ShardIteratorType.UNKNOWN_TO_SDK_VERSION);

        streamStatusMap.put(StreamStatus.DISABLED, software.amazon.awssdk.services.kinesis.model.StreamStatus.ACTIVE);
        streamStatusMap.put(StreamStatus.DISABLING, software.amazon.awssdk.services.kinesis.model.StreamStatus.ACTIVE);
        streamStatusMap.put(StreamStatus.ENABLED, software.amazon.awssdk.services.kinesis.model.StreamStatus.ACTIVE);
        streamStatusMap.put(StreamStatus.ENABLING, software.amazon.awssdk.services.kinesis.model.StreamStatus.CREATING);
    }

    public ListStreamsRequest translate(software.amazon.awssdk.services.kinesis.model.ListStreamsRequest kinesisRequest) {
        return ListStreamsRequest.builder()
            .exclusiveStartStreamArn(kinesisRequest.exclusiveStartStreamName())
            .limit(kinesisRequest.limit())
            .build();
    }

    public GetShardIteratorRequest translate(software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest kinesisRequest) {
        return GetShardIteratorRequest.builder()
            .sequenceNumber(kinesisRequest.startingSequenceNumber())
            .shardId(kinesisRequest.shardId())
            .shardIteratorType(shardIteratorTypeMap.get(kinesisRequest.shardIteratorType()))
            .streamArn(kinesisRequest.streamName())
            .build();
    }

    public DescribeStreamRequest translate(software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest kinesisRequest) {
        return DescribeStreamRequest.builder()
            .exclusiveStartShardId(kinesisRequest.exclusiveStartShardId())
            .limit(kinesisRequest.limit())
            .streamArn(kinesisRequest.streamName())
            .build();
    }

    public GetRecordsRequest translate(software.amazon.awssdk.services.kinesis.model.GetRecordsRequest kinesisRequest) {
        return GetRecordsRequest.builder()
            .limit(kinesisRequest.limit())
            .shardIterator(kinesisRequest.shardIterator())
            .build();
    }

    public software.amazon.awssdk.services.kinesis.model.Shard translate(Shard shard) {
        software.amazon.awssdk.services.kinesis.model.SequenceNumberRange
            sequenceNumberRange =
            software.amazon.awssdk.services.kinesis.model.SequenceNumberRange.builder()
                .startingSequenceNumber(shard.sequenceNumberRange().startingSequenceNumber())
                .endingSequenceNumber(shard.sequenceNumberRange().endingSequenceNumber())
                .build();

        software.amazon.awssdk.services.kinesis.model.HashKeyRange
            hashKeyRange =
            software.amazon.awssdk.services.kinesis.model.HashKeyRange.builder()
                .startingHashKey(java.math.BigInteger.ZERO.toString())
                .endingHashKey(java.math.BigInteger.ONE.toString())
                .build();

        return software.amazon.awssdk.services.kinesis.model.Shard.builder()
            .shardId(shard.shardId())
            .parentShardId(shard.parentShardId())
            .sequenceNumberRange(sequenceNumberRange)
            .hashKeyRange(hashKeyRange)
            .build();
    }

    //if-else/switch vs maps
    public software.amazon.awssdk.services.kinesis.model.StreamDescription translate(StreamDescription dynamoDBDescription) {
        return software.amazon.awssdk.services.kinesis.model.StreamDescription.builder()
            .shards(dynamoDBDescription.shards().stream().map(this::translate).collect(Collectors.toList()))
            .streamARN(dynamoDBDescription.streamArn())
            .streamStatus(streamStatusMap.get(dynamoDBDescription.streamStatus()))
            .hasMoreShards(dynamoDBDescription.lastEvaluatedShardId() != null)
            .build();
    }

    public software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse translate(DescribeStreamResponse dynamoDBResponse) {
        return software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse.builder()
            .streamDescription(this.translate(dynamoDBResponse.streamDescription()))
            .build();
    }

    public software.amazon.awssdk.services.kinesis.model.ListStreamsResponse translate(ListStreamsResponse dynamoDBResponse) {
        return software.amazon.awssdk.services.kinesis.model.ListStreamsResponse.builder()
            .streamNames(dynamoDBResponse.streams().stream().map(Stream::streamArn).collect(Collectors.toList()))
            .hasMoreStreams(dynamoDBResponse.lastEvaluatedStreamArn() != null)
            .build();
    }

    public software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse translate(GetShardIteratorResponse dynamoDBResponse) {
        return software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse.builder()
            .shardIterator(dynamoDBResponse.shardIterator())
            .build();
    }

    public software.amazon.awssdk.services.kinesis.model.Record translate(Record record) {
        //MAPPER.registerModule(new JavaTimeModule());
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            return software.amazon.awssdk.services.kinesis.model.Record.builder()
                .approximateArrivalTimestamp(record.dynamodb().approximateCreationDateTime())
                .sequenceNumber(record.dynamodb().sequenceNumber())
                .data(SdkBytes.fromByteArray(MAPPER.writeValueAsBytes(record.toBuilder())))
                .build();
        } catch (JsonProcessingException e) {
            final String errorMessage = "Failed to serialize stream record to JSON";
            // LOG.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse translate(GetRecordsResponse dynamoDBResponse) {
        return software.amazon.awssdk.services.kinesis.model.GetRecordsResponse.builder()
            .records(dynamoDBResponse.records().stream().map(this::translate).collect(Collectors.toList()))
            .nextShardIterator(dynamoDBResponse.nextShardIterator())
            .build();
    }
}
