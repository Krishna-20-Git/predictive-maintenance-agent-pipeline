package com.krishna.orchestrator.service;

import com.krishna.orchestrator.entity.Alert;
import com.krishna.orchestrator.repository.AlertRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

/**
 * Day 9 — consumes stream:scored-alerts and persists each alert into Postgres.
 *
 * Key Jedis 7.x API notes:
 *  - StreamEntry is in redis.clients.jedis.resps (moved from jedis root)
 *  - StreamEntryID stays in redis.clients.jedis
 *  - xreadGroup takes varargs Map.Entry<String, StreamEntryID>, not a Map
 */
@Service
public class StreamConsumerService {

    private static final Logger log = LoggerFactory.getLogger(StreamConsumerService.class);

    private static final String STREAM_KEY   = "stream:scored-alerts";
    private static final String CONSUMER_GROUP = "spring-orchestrator";
    private static final String CONSUMER_NAME  = "orchestrator-1";
    private static final int    BATCH_SIZE     = 20;

    private final JedisPooled        jedis;
    private final AlertRepository    alertRepository;

    @Value("${app.alert-consumer.enabled:true}")
    private boolean enabled;

    private boolean groupInitialized = false;

    public StreamConsumerService(JedisPooled jedis, AlertRepository alertRepository) {
        this.jedis           = jedis;
        this.alertRepository = alertRepository;
    }

    private void ensureConsumerGroup() {
        if (groupInitialized) return;
        try {
            jedis.xgroupCreate(STREAM_KEY, CONSUMER_GROUP, new StreamEntryID(0, 0), true);
            log.info("Created consumer group '{}' on '{}'", CONSUMER_GROUP, STREAM_KEY);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("BUSYGROUP")) {
                log.info("Consumer group '{}' already exists", CONSUMER_GROUP);
            } else {
                log.error("Failed to create consumer group", e);
                throw e;
            }
        }
        groupInitialized = true;
    }

    @Scheduled(fixedDelay = 2000)
    public void pollAndProcess() {
        if (!enabled) return;
        ensureConsumerGroup();

        XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(BATCH_SIZE);

        Map<String, StreamEntryID> streams = new java.util.HashMap<>();
        streams.put(STREAM_KEY, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY);

        List<Map.Entry<String, List<StreamEntry>>> response;
        try {
            response = jedis.xreadGroup(CONSUMER_GROUP, CONSUMER_NAME, params, streams);
        } catch (Exception e) {
            log.error("Error reading from stream '{}'", STREAM_KEY, e);
            return;
        }

        if (response == null || response.isEmpty()) return;

        int processed = 0;
        for (Map.Entry<String, List<StreamEntry>> streamResult : response) {
            for (StreamEntry entry : streamResult.getValue()) {
                processEntry(entry);
                processed++;
            }
        }

        if (processed > 0) {
            log.info("Processed {} alert(s) from '{}'", processed, STREAM_KEY);
        }
    }

    private void processEntry(StreamEntry entry) {
        try {
            Map<String, String> fields = entry.getFields();

            Integer machineId   = Integer.parseInt(fields.get("machine_id"));
            Double  probability = Double.parseDouble(fields.get("failure_probability"));

            Integer cyclePosition = (fields.containsKey("cycle_position") && !fields.get("cycle_position").isEmpty())
                    ? Integer.parseInt(fields.get("cycle_position")) : null;

            Double sourceTimestamp = (fields.containsKey("source_timestamp") && !fields.get("source_timestamp").isEmpty())
                    ? Double.parseDouble(fields.get("source_timestamp")) : null;

            alertRepository.save(new Alert(machineId, probability, cyclePosition, sourceTimestamp));
            jedis.xack(STREAM_KEY, CONSUMER_GROUP, entry.getID());

        } catch (Exception e) {
            // Deliberately NOT acking — malformed messages stay pending (visible in XPENDING)
            // rather than being silently dropped.
            log.error("Failed to process entry {}: {}", entry.getID(), e.getMessage(), e);
        }
    }
}
