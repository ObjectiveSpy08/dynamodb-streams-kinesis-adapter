/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/*
 * Even with good serialization support from AWS SDK 2, we need mixins to correctly map fields like record
 * .eventNameAsString
 * which don't follow the naming convention of field-getter-setter combinations.
 * Eg. the field record.eventID has getter getEventId and setter setEventID(in the builder). This allows jackson to
 * correctly determine
 * the field names from those methods(that is how Jackson serialization works). But the field eventName has getter
 * getEventNameAsString and setter as setEventName which
 * causes jackson to throw UnrecognizedPropertyException.
 *
 */
public class RecordObjectMapper extends ObjectMapper {
    public static final String L = "L";
    public static final String M = "M";
    public static final String BS = "BS";
    public static final String NS = "NS";
    public static final String SS = "SS";
    public static final String BOOL = "BOOL";
    public static final String NULL = "NULL";
    public static final String B = "B";
    public static final String N = "N";
    public static final String S = "S";
    public static final String OLD_IMAGE = "OldImage";
    public static final String NEW_IMAGE = "NewImage";
    public static final String STREAM_VIEW_TYPE = "StreamViewType";
    public static final String OPERATION_TYPE = "OperationType";
    public static final String SEQUENCE_NUMBER = "SequenceNumber";
    public static final String SIZE_BYTES = "SizeBytes";
    public static final String KEYS = "Keys";
    public static final String EVENT_NAME = "eventName";
    public static final String EVENT_SOURCE = "eventSource";
    public static final String EVENT_VERSION = "eventVersion";
    public static final String AWS_REGION = "awsRegion";
    public static final String DYNAMODB = "dynamodb";
    public static final String EVENT_ID = "eventID";

    public static final String APPROXIMATE_CREATION_DATE_TIME = "ApproximateCreationDateTime";

    public RecordObjectMapper(JavaTimeModule timeModule) {
        super();

        // Don't serialize things that are null
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        this.registerModule(timeModule);

        this.addMixIn(AttributeValue.serializableBuilderClass(), AttributeValueMixIn.class);
        this.addMixIn(Record.serializableBuilderClass(), RecordMixIn.class);
        this.addMixIn(StreamRecord.serializableBuilderClass(), StreamRecordMixIn.class);
    }

    private static abstract class RecordMixIn {

        @JsonProperty(EVENT_ID) public abstract String getEventID();

        @JsonProperty(EVENT_ID) public abstract void setEventID(String eventID);

        @JsonProperty(EVENT_NAME) public abstract String getEventNameAsString();

        @JsonProperty(EVENT_NAME) public abstract void setEventName(String eventName);

        @JsonProperty(EVENT_VERSION) public abstract String getEventVersion();

        @JsonProperty(EVENT_VERSION) public abstract void setEventVersion(String eventVersion);

        @JsonProperty(EVENT_SOURCE) public abstract String getEventSource();

        @JsonProperty(EVENT_SOURCE) public abstract void setEventSource(String eventSource);

        @JsonProperty(AWS_REGION) public abstract String getAwsRegion();

        @JsonProperty(AWS_REGION) public abstract void setAwsRegion(String awsRegion);

        @JsonProperty(DYNAMODB) public abstract StreamRecord getDynamodb();

        @JsonProperty(DYNAMODB) public abstract void setDynamodb(StreamRecord dynamodb);
    }

    private static abstract class StreamRecordMixIn {

        @JsonProperty(APPROXIMATE_CREATION_DATE_TIME) public abstract Instant getApproximateCreationDateTime();

        @JsonProperty(APPROXIMATE_CREATION_DATE_TIME)
        public abstract void setApproximateCreationDateTime(Instant approximateCreationDateTime);

        @JsonProperty(KEYS) public abstract Map<String, AttributeValue.Builder> getKeys();

        @JsonProperty(KEYS) public abstract void setKeys(Map<String, AttributeValue.Builder> keys);

        @JsonProperty(NEW_IMAGE) public abstract Map<String, AttributeValue.Builder> getNewImage();

        @JsonProperty(NEW_IMAGE) public abstract void setNewImage(Map<String, AttributeValue.Builder> newImage);

        @JsonProperty(OLD_IMAGE) public abstract Map<String, AttributeValue.Builder> getOldImage();

        @JsonProperty(OLD_IMAGE) public abstract void setOldImage(Map<String, AttributeValue.Builder> oldImage);

        @JsonProperty(SEQUENCE_NUMBER) public abstract String getSequenceNumber();

        @JsonProperty(SEQUENCE_NUMBER) public abstract void setSequenceNumber(String sequenceNumber);

        @JsonProperty(SIZE_BYTES) public abstract Long getSizeBytes();

        @JsonProperty(SIZE_BYTES) public abstract void setSizeBytes(Long sizeBytes);

        @JsonProperty(STREAM_VIEW_TYPE) public abstract String getStreamViewTypeAsString();

        @JsonProperty(STREAM_VIEW_TYPE) public abstract void setStreamViewType(String streamViewType);

    }

    private static abstract class AttributeValueMixIn {
        @JsonProperty(S) public abstract String getS();

        @JsonProperty(S) public abstract void setS(String s);

        @JsonProperty(N) public abstract String getN();

        @JsonProperty(N) public abstract void setN(String n);

        @JsonProperty(B) public abstract ByteBuffer getB();

        @JsonProperty(B) public abstract void setB(ByteBuffer b);

        @JsonProperty(NULL) public abstract Boolean getNul();

        @JsonProperty(NULL) public abstract void setNul(Boolean nU);

        @JsonProperty(BOOL) public abstract Boolean getBool();

        @JsonProperty(BOOL) public abstract void setBool(Boolean bO);

        @JsonProperty(SS) public abstract Collection<String> getSs();

        @JsonProperty(SS) public abstract void setSs(Collection<String> sS);

        @JsonProperty(NS) public abstract Collection<String> getNs();

        @JsonProperty(NS) public abstract void setNs(Collection<String> nS);

        @JsonProperty(BS) public abstract List<ByteBuffer> getBs();

        @JsonProperty(BS) public abstract void setBs(Collection<ByteBuffer> bS);

        @JsonProperty(M) public abstract Map<String, AttributeValue.Builder> getM();

        @JsonProperty(M) public abstract void setM(Map<String, AttributeValue.Builder> val);

        @JsonProperty(L) public abstract Collection<AttributeValue.Builder> getL();

        @JsonProperty(L) public abstract void setL(Collection<AttributeValue.Builder> val);
    }
}
