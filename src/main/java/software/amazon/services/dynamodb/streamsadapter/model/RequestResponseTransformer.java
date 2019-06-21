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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.services.dynamodb.streamsadapter.model.RecordObjectMapper;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Stream;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import software.amazon.awssdk.services.dynamodb.model.StreamDescription;

import java.util.Map;
import java.util.EnumMap;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;

// TODO: No static methods. immutable map
public class RequestResponseTransformer {
    private static final Map<software.amazon.awssdk.services.kinesis.model.ShardIteratorType, ShardIteratorType> shardIteratorTypeMap =
        new EnumMap<>(software.amazon.awssdk.services.kinesis.model.ShardIteratorType.class);

    private static final Map<StreamStatus, software.amazon.awssdk.services.kinesis.model.StreamStatus> streamStatusMap =
        new EnumMap<>(StreamStatus.class);

    private static final ObjectMapper MAPPER = new RecordObjectMapper();

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

    public static ListStreamsRequest listStreamRequest(software.amazon.awssdk.services.kinesis.model.ListStreamsRequest kinesisRequest) {
        return ListStreamsRequest.builder()
            .exclusiveStartStreamArn(kinesisRequest.exclusiveStartStreamName())
            .limit(kinesisRequest.limit())
            .build();
    }

    public static GetShardIteratorRequest getShardIteratorRequest(software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest kinesisRequest) {
        return GetShardIteratorRequest.builder()
            .sequenceNumber(kinesisRequest.startingSequenceNumber())
            .shardId(kinesisRequest.shardId())
            .shardIteratorType(shardIteratorTypeMap.get(kinesisRequest.shardIteratorType()))
            .streamArn(kinesisRequest.streamName())
            .build();
    }

    public static DescribeStreamRequest describeStreamRequest(software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest kinesisRequest) {
        return DescribeStreamRequest.builder()
            .exclusiveStartShardId(kinesisRequest.exclusiveStartShardId())
            .limit(kinesisRequest.limit())
            .streamArn(kinesisRequest.streamName())
            .build();
    }

    public static GetRecordsRequest getRecordsRequest(software.amazon.awssdk.services.kinesis.model.GetRecordsRequest kinesisRequest) {
        return GetRecordsRequest.builder()
            .limit(kinesisRequest.limit())
            .shardIterator(kinesisRequest.shardIterator())
            .build();
    }

    public static software.amazon.awssdk.services.kinesis.model.Shard getShard(Shard shard) {
        software.amazon.awssdk.services.kinesis.model.SequenceNumberRange sequenceNumberRange = software.amazon.awssdk.services.kinesis.model.SequenceNumberRange.builder()
            .startingSequenceNumber(shard.sequenceNumberRange().startingSequenceNumber())
            .endingSequenceNumber(shard.sequenceNumberRange().endingSequenceNumber())
            .build();

        software.amazon.awssdk.services.kinesis.model.HashKeyRange hashKeyRange = software.amazon.awssdk.services.kinesis.model.HashKeyRange.builder()
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
    public static software.amazon.awssdk.services.kinesis.model.StreamDescription streamDescription(StreamDescription dynamoDBDescription) {
        return software.amazon.awssdk.services.kinesis.model.StreamDescription.builder()
            .shards(dynamoDBDescription.shards().stream().map(RequestResponseTransformer::getShard).collect(Collectors.toList()))
            .streamARN(dynamoDBDescription.streamArn())
            .streamStatus(streamStatusMap.get(dynamoDBDescription.streamStatus()))
            .hasMoreShards(dynamoDBDescription.lastEvaluatedShardId() != null)
            .build();
    }

    public static software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse describeStreamResponse(DescribeStreamResponse dynamoDBResponse) {
        return software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse.builder()
            .streamDescription(RequestResponseTransformer.streamDescription(dynamoDBResponse.streamDescription()))
            .build();
    }

    public static software.amazon.awssdk.services.kinesis.model.ListStreamsResponse listStreamsResponse(ListStreamsResponse dynamoDBResponse) {
        return software.amazon.awssdk.services.kinesis.model.ListStreamsResponse.builder()
            .streamNames(dynamoDBResponse.streams().stream().map(Stream::streamArn).collect(Collectors.toList()))
            .hasMoreStreams(dynamoDBResponse.lastEvaluatedStreamArn() != null)
            .build();
    }

    public static software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse shardIteratorResponse(GetShardIteratorResponse dynamoDBResponse) {
        return software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse.builder()
            .shardIterator(dynamoDBResponse.shardIterator())
            .build();
    }

    public static software.amazon.awssdk.services.kinesis.model.Record getRecord(Record record) {
        String value = record.dynamodb().newImage().toString();
        //byte[] result = new byte[]{value};
        try {
            return software.amazon.awssdk.services.kinesis.model.Record.builder()
                .approximateArrivalTimestamp(record.dynamodb().approximateCreationDateTime())
                .sequenceNumber(record.dynamodb().sequenceNumber())
                //.data(SdkBytes.fromByteArray(MAPPER.writeValueAsBytes(record)))
                .data(SdkBytes.fromUtf8String(value))
                //.data(SdkBytes.fromByteArray(result))
                .build();
        }
//        catch (JsonProcessingException e) {
//            final String errorMessage = "Failed to serialize stream record to JSON";
//            // LOG.error(errorMessage, e);
//            throw new RuntimeException(errorMessage, e);
//        }
        catch (Exception d) {
            d.printStackTrace();
            System.out.println("hole");
            throw new RuntimeException("sdasadasdas", d);
        }
    }

    public static software.amazon.awssdk.services.kinesis.model.GetRecordsResponse getRecordsResponse(GetRecordsResponse dynamoDBResponse) {
        //System.out.println("getRecords transaformation");
        return software.amazon.awssdk.services.kinesis.model.GetRecordsResponse.builder()
            .records(dynamoDBResponse.records().stream().map(RequestResponseTransformer::getRecord).collect(Collectors.toList()))
            .nextShardIterator(dynamoDBResponse.nextShardIterator())
            .build();
    }
}
