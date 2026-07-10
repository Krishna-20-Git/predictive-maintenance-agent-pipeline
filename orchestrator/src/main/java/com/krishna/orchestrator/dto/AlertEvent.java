package com.krishna.orchestrator.dto;

import com.krishna.orchestrator.entity.Alert;
import java.time.Instant;

/**
 * Payload pushed over WebSocket to /topic/alerts whenever a new alert
 * is saved. Deliberately lighter than AlertResponse — the WebSocket feed
 * is for live dashboard updates, not historical lookups, so it only needs
 * the fields the triage board actually displays in real time.
 *
 * Jackson serializes this to JSON automatically when passed to
 * SimpMessagingTemplate.convertAndSend().
 */
public record AlertEvent(
        Long id,
        Integer machineId,
        Double failureProbability,
        Boolean failureSoon,
        Integer cyclePosition,
        Instant receivedAt,
        String status
) {
    public static AlertEvent from(Alert alert) {
        return new AlertEvent(
                alert.getId(),
                alert.getMachineId(),
                alert.getFailureProbability(),
                alert.getFailureProbability() >= 0.5,
                alert.getCyclePosition(),
                alert.getReceivedAt(),
                alert.getStatus().name()
        );
    }
}
