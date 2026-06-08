package com.standard.ibte.bentech;

import com.standard.ibte.bentech.client.BentechApiResponse;
import com.standard.ibte.bentech.client.BentechClient;
import com.standard.ibte.bentech.enums.ApiResultStatus;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class BentechSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(BentechSinkTask.class);
    private BentechSinkConfig config;
    private BentechClient client;
    private BentechResponsePublisher responsePublisher;

    @Override
    public String version() {
        return new BentechSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new BentechSinkConfig(props);
        log.info("BentechSinkTask starting – connector: {}, secrets mount: {}, content-type: {}",
                config.getString(BentechSinkConfig.CONNECTOR_CONNECTOR_NAME),
                config.getString(BentechSinkConfig.CONNECTOR_SECRETS_MOUNT_PATH),
                config.getString(BentechSinkConfig.CONNECTOR_REQUEST_CONTENT_TYPE));
        try {
            this.client            = new BentechClient(config);
            this.responsePublisher = new BentechResponsePublisher(config);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialise BentechSinkTask", e);
        }
        log.info("BentechSinkTask started successfully");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.debug("Received {} record(s) to process", records.size());
        for (SinkRecord record : records) {
            log.info("Message consumed – kafka-key: {}, topic: {}, partition: {}, offset: {}, status: CONSUMED",
                    record.key(), record.topic(), record.kafkaPartition(), record.kafkaOffset());
            processRecord(record);
        }
    }

    private void processRecord(SinkRecord record) {
        int maxRetries        = config.getInt(BentechSinkConfig.CONNECTOR_RETRY_MAX);
        long retryBackoffMs   = config.getLong(BentechSinkConfig.CONNECTOR_RETRY_BACKOFF_MS);
        BentechApiResponse result = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                result = client.sendRecord(record);

                if (result.status == ApiResultStatus.SYSTEM_ERROR && attempt < maxRetries) {
                    log.warn("System error on attempt {}/{} – retrying. topic: {}, offset: {}",
                            attempt + 1, maxRetries, record.topic(), record.kafkaOffset());
                    sleepQuietly(retryBackoffMs * (long) (attempt + 1));
                    continue;
                }

                break;

            } catch (IOException e) {
                if (attempt < maxRetries) {
                    log.warn("IOException on attempt {}/{} – retrying. topic: {}, offset: {}: {}",
                            attempt + 1, maxRetries, record.topic(), record.kafkaOffset(), e.getMessage());
                    sleepQuietly(retryBackoffMs * (long) (attempt + 1));
                } else {
                    log.error("All retries exhausted – IOException on attempt {}/{}, topic: {}, offset: {}: {}",
                            attempt + 1, maxRetries, record.topic(), record.kafkaOffset(), e.getMessage(), e);
                    result = new BentechApiResponse(ApiResultStatus.SYSTEM_ERROR, 0, null, e.getMessage(), attempt);
                }
            }
        }

        if (result == null) {
            // Should not happen, but guard against it
            responsePublisher.publishInternalError(record, "Null result after retry loop");
            return;
        }

        switch (result.status) {
            case SUCCESS:
                log.info("Publishing success response – topic: {}, offset: {}",
                        record.topic(), record.kafkaOffset());
                responsePublisher.publishSuccess(record, result);
                break;
            case DATA_ERROR:
                log.warn("Data error – publishing to error topic. topic: {}, offset: {}",
                        record.topic(), record.kafkaOffset());
                responsePublisher.publishError(record, result);
                break;
            case SYSTEM_ERROR:
                log.error("Retry exhausted – publishing to DLQ. topic: {}, partition: {}, offset: {}",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset());
                responsePublisher.publishDlq(record, result, maxRetries);
                break;
        }
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop() {
        log.info("BentechSinkTask stopping");
        if (client != null) client.close();
        if (responsePublisher != null) responsePublisher.close();
        log.info("BentechSinkTask stopped successfully");
    }
}
