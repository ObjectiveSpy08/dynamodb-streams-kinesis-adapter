package software.amazon.services.dynamodb.streamsadapter.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.services.dynamodb.streamsadapter.model.RecordObjectMapper;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.List;
import java.util.stream.Collectors;

public interface DynamoDBStreamsShardRecordProcessor extends ShardRecordProcessor {
    ObjectMapper MAPPER = new RecordObjectMapper(new JavaTimeModule());

    default void processRecords(final ProcessRecordsInput processRecordsInput) {
        processRecords(processRecordsInput, processRecordsInput.records().stream().map(record -> {
            byte[] arr = new byte[record.data().remaining()];
            try {
                record.data().get(arr);
                return MAPPER.readValue(arr, Record.serializableBuilderClass()).build();
            } catch (IOException | BufferUnderflowException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList()));
    }

    void processRecords(ProcessRecordsInput processRecordsInput, List<Record> records);
}
