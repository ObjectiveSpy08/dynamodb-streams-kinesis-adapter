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
package software.amazon.services.dynamodb.streamsadapter.exceptions;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.services.dynamodb.streamsadapter.DynamoDBStreamsAsyncClientAdapter.SkipRecordsBehavior;

public class ExceptionTranslator {

    private static final String
        TRIMMED_DATA_KCL_RETRY_MESSAGE =
        "Attempted to get a shard iterator for a trimmed shard. Data has been lost";

    private static final String
        TRIMMED_DATA_LOST_EXCEPTION_MESSAGE =
        "Attempted to access trimmed data. Data has been lost";

    /**
     * Error message used for ThrottlingException by the Amazon DynamoDB Streams service.
     */
    private static final String DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE = "ThrottlingException";

    /**
     * Empty String used when an Exception requires an error message, but none is supplied.
     */
    private static final String EMPTY_STRING = "";

    /**
     * Builds the error message for a transformed exception. Returns the original error message or an empty String if
     * the original error message was null.
     *
     * @param exception The original exception
     * @return The error message for a transformed exception. Returns the original error message or an empty String if
     * the original error message was null.
     */
    private String buildErrorMessage(AwsServiceException exception) {
        if (exception.getMessage() == null) {
            return EMPTY_STRING;
        }
        return exception.getMessage();
    }

    private AwsServiceException buildException(AwsServiceException.Builder builder, AwsServiceException awsException) {
        return builder.message(buildErrorMessage(awsException))
            .awsErrorDetails(awsException.awsErrorDetails())
            .cause(awsException)
            .requestId(awsException.requestId())
            .statusCode(awsException.statusCode())
            .build();
    }

    private AwsServiceException translate(InternalServerErrorException exception) {
        return buildException(AwsServiceException.builder(), exception);
    }

    private AwsServiceException translate(ResourceNotFoundException exception) {
        return buildException(software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.builder(),
            exception);
    }

    private AwsServiceException translate(ExpiredIteratorException exception) {
        return buildException(software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException.builder(),
            exception);
    }

    private AwsServiceException translate(LimitExceededException exception) {
        return buildException(software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException.builder(),
            exception);
    }

    private AwsServiceException translate(TrimmedDataAccessException exception) {
        return buildException(software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException.builder(),
            exception);
    }

    private AwsServiceException translate(DynamoDbException exception) {
        return buildException(software.amazon.awssdk.services.kinesis.model.KinesisException.builder(), exception);
    }

    public RuntimeException translateDescribeStreamException(final AwsServiceException awsException) {
        if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(awsException.awsErrorDetails().errorCode())) {
            return buildException(software.amazon.awssdk.services.kinesis.model.LimitExceededException.builder(),
                awsException);
        }

        final ExceptionManager exceptionManager = new ExceptionManager();
        exceptionManager.add(InternalServerErrorException.class, this::translate);
        exceptionManager.add(ResourceNotFoundException.class, this::translate);
        exceptionManager.add(DynamoDbException.class, this::translate);
        return exceptionManager.apply(awsException);
    }

    public RuntimeException translateGetRecordsException(final AwsServiceException awsException,
        SkipRecordsBehavior skipRecordsBehavior) {
        if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(awsException.awsErrorDetails().errorCode())) {
            return buildException(software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException.builder(),
                awsException);
        }
        final ExceptionManager exceptionManager = new ExceptionManager();
        exceptionManager.add(InternalServerErrorException.class, this::translate);
        exceptionManager.add(ExpiredIteratorException.class, this::translate);
        exceptionManager.add(LimitExceededException.class, this::translate);
        exceptionManager.add(ResourceNotFoundException.class, this::translate);
        exceptionManager.add(TrimmedDataAccessException.class, (exception) -> {
            if (skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) { // Data loss is acceptable
                return translate(exception);
            } else {
                return new UnableToReadMoreRecordsException(TRIMMED_DATA_LOST_EXCEPTION_MESSAGE, awsException);
            }
        });
        exceptionManager.add(DynamoDbException.class, this::translate);
        return exceptionManager.apply(awsException);
    }

    public RuntimeException translateGetShardIteratorException(AwsServiceException awsException,
        SkipRecordsBehavior skipRecordsBehavior) {
        if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(awsException.awsErrorDetails().errorCode())) {
            return buildException(software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException.builder(),
                awsException);
        }
        final ExceptionManager exceptionManager = new ExceptionManager();
        exceptionManager.add(InternalServerErrorException.class, this::translate);
        exceptionManager.add(ResourceNotFoundException.class, (exception) -> {
            if (skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                return translate(exception);
            } else {
                return new UnableToReadMoreRecordsException(TRIMMED_DATA_KCL_RETRY_MESSAGE, exception);
            }
        });
        exceptionManager.add(TrimmedDataAccessException.class, (exception) -> {
            if (skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) { // Data loss is acceptable
                return translate(exception);
            } else {
                return new UnableToReadMoreRecordsException(TRIMMED_DATA_KCL_RETRY_MESSAGE, awsException);
            }
        });
        exceptionManager.add(DynamoDbException.class, this::translate);

        return exceptionManager.apply(awsException);
    }

    public RuntimeException translateListStreamsException(AwsServiceException awsException) {
        if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(awsException.awsErrorDetails().errorCode())) {
            return buildException(software.amazon.awssdk.services.kinesis.model.LimitExceededException.builder(),
                awsException);
        }
        final ExceptionManager exceptionManager = new ExceptionManager();
        exceptionManager.add(InternalServerErrorException.class, this::translate);
        exceptionManager.add(ResourceNotFoundException.class, this::translate);
        exceptionManager.add(DynamoDbException.class, this::translate);
        return exceptionManager.apply(awsException);
    }

}
