package software.amazon.services.dynamodb.streamsadapter;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import software.amazon.services.dynamodb.streamsadapter.leases.StreamsLeaseManagementFactory;
import software.amazon.services.dynamodb.streamsadapter.processor.DynamoDBStreamsShardRecordProcessorFactory;



@RunWith(PowerMockRunner.class) @PrepareForTest(DynamoDBStreamsFactory.class) public class DynamoDBStreamsFactoryTest {
    private static final String streamName = "stream";
    private static final String applicationName = "application";
    @Mock private KinesisAsyncClient kinesisClient;
    @Mock private ConfigsBuilder configsBuilder;
    @Mock private DynamoDbAsyncClient dynamoDBClient;
    @Mock private CloudWatchAsyncClient cloudWatchClient;
    @Mock private String workerIdentifier;
    @Mock private DynamoDBStreamsShardRecordProcessorFactory streamsShardRecordProcessorFactory;
    @Mock private LeaseManagementConfig leaseManagementConfig;
    @Mock private RetrievalConfig retrievalConfig;
    @Mock private CheckpointConfig checkpointConfig;
    @Mock private CoordinatorConfig coordinatorConfig;
    @Mock private LifecycleConfig lifecycleConfig;
    @Mock private MetricsConfig metricsConfig;
    @Mock private ProcessorConfig processorConfig;

    @Test public void createConfigsBuilder() throws Exception {
        // Phase 1: Setup
        ConfigsBuilder mockConfigsBuilder = PowerMockito.mock(ConfigsBuilder.class);
        PowerMockito.whenNew(ConfigsBuilder.class).withAnyArguments().thenReturn(mockConfigsBuilder);

        // Phase 2: Exercise
        ConfigsBuilder
            configsBuilder =
            DynamoDBStreamsFactory.createConfigsBuilder(streamName,
                applicationName,
                kinesisClient,
                dynamoDBClient,
                cloudWatchClient,
                workerIdentifier,
                streamsShardRecordProcessorFactory);

        // Phase 3: Verification
        PowerMockito.verifyNew(ConfigsBuilder.class)
            .withArguments(streamName,
                applicationName,
                kinesisClient,
                dynamoDBClient,
                cloudWatchClient,
                workerIdentifier,
                streamsShardRecordProcessorFactory);
        Assert.assertEquals(mockConfigsBuilder, configsBuilder);
    }

    @Test public void createScheduler_configsBuilderParam() throws Exception {
        // Phase 1: Setup
        Scheduler mockScheduler = PowerMockito.mock(Scheduler.class);
        StreamsLeaseManagementFactory mockStreamsLeaseManagementFactory = PowerMockito.mock(StreamsLeaseManagementFactory.class);
        PollingConfig mockPollingConfig = PowerMockito.mock(PollingConfig.class);

        PowerMockito.whenNew(Scheduler.class).withAnyArguments().thenReturn(mockScheduler);
        PowerMockito.whenNew(StreamsLeaseManagementFactory.class).withAnyArguments().thenReturn(mockStreamsLeaseManagementFactory);
        PowerMockito.whenNew(PollingConfig.class).withAnyArguments().thenReturn(mockPollingConfig);

        PowerMockito.when(configsBuilder.leaseManagementConfig()).thenReturn(leaseManagementConfig);
        PowerMockito.when(configsBuilder.retrievalConfig()).thenReturn(retrievalConfig);
        PowerMockito.when(leaseManagementConfig.leaseManagementFactory(mockStreamsLeaseManagementFactory)).thenReturn(leaseManagementConfig);
        PowerMockito.when(retrievalConfig.retrievalSpecificConfig(mockPollingConfig)).thenReturn(retrievalConfig);

        // Phase 2: Exercise
        Scheduler scheduler = DynamoDBStreamsFactory.createScheduler(configsBuilder);

        // Phase 3: Verification
        PowerMockito.verifyNew(Scheduler.class)
            .withArguments(configsBuilder.checkpointConfig(), configsBuilder.coordinatorConfig(),
                leaseManagementConfig, configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(), configsBuilder.processorConfig(),
                retrievalConfig);

        PowerMockito.verifyNew(StreamsLeaseManagementFactory.class)
            .withArguments(leaseManagementConfig.leaseManagementFactory(), leaseManagementConfig.kinesisClient(), leaseManagementConfig.streamName());

        PowerMockito.verifyNew(PollingConfig.class)
            .withArguments(configsBuilder.streamName(),configsBuilder.kinesisClient());

        verify(leaseManagementConfig).leaseManagementFactory(mockStreamsLeaseManagementFactory);
        verify(retrievalConfig).retrievalSpecificConfig(mockPollingConfig);

        Assert.assertEquals(mockScheduler, scheduler);
    }

    @Test public void createScheduler_allConfigParam() throws Exception {
        // Phase 1: Setup
        Scheduler mockScheduler = PowerMockito.mock(Scheduler.class);
        StreamsLeaseManagementFactory mockStreamsLeaseManagementFactory = PowerMockito.mock(StreamsLeaseManagementFactory.class);
        PollingConfig mockPollingConfig = PowerMockito.mock(PollingConfig.class);

        PowerMockito.whenNew(Scheduler.class).withAnyArguments().thenReturn(mockScheduler);
        PowerMockito.whenNew(StreamsLeaseManagementFactory.class).withAnyArguments().thenReturn(mockStreamsLeaseManagementFactory);
        PowerMockito.whenNew(PollingConfig.class).withAnyArguments().thenReturn(mockPollingConfig);

        PowerMockito.when(leaseManagementConfig.leaseManagementFactory(mockStreamsLeaseManagementFactory)).thenReturn(leaseManagementConfig);
        PowerMockito.when(retrievalConfig.retrievalSpecificConfig(mockPollingConfig)).thenReturn(retrievalConfig);

        // Phase 2: Exercise
        Scheduler scheduler = DynamoDBStreamsFactory.createScheduler(checkpointConfig,
            coordinatorConfig,
            leaseManagementConfig,
            lifecycleConfig,
            metricsConfig,
            processorConfig,
            retrievalConfig);

        // Phase 3: Verification
        PowerMockito.verifyNew(Scheduler.class)
            .withArguments(checkpointConfig, coordinatorConfig,
                leaseManagementConfig, lifecycleConfig,
                metricsConfig, processorConfig,
                retrievalConfig);

        PowerMockito.verifyNew(StreamsLeaseManagementFactory.class)
            .withArguments(leaseManagementConfig.leaseManagementFactory(), leaseManagementConfig.kinesisClient(), leaseManagementConfig.streamName());

        PowerMockito.verifyNew(PollingConfig.class)
            .withArguments(configsBuilder.streamName(),configsBuilder.kinesisClient());

        verify(leaseManagementConfig).leaseManagementFactory(mockStreamsLeaseManagementFactory);
        verify(retrievalConfig).retrievalSpecificConfig(mockPollingConfig);

        Assert.assertEquals(mockScheduler, scheduler);
    }
}
