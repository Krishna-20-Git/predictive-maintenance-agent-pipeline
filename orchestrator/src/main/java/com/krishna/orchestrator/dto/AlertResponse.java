package com.krishna.orchestrator.dto;

import com.krishna.orchestrator.entity.Alert;
import java.time.Instant;

/**
 * What GET /api/alerts actually returns. Deliberately separate from the
 * Alert entity — returning JPA entities directly from a controller is a
 * common anti-pattern (it couples your wire format to your DB schema, and
 * risks accidentally serializing lazy-loaded fields). This record is the
 * stable public contract; the entity can change shape without breaking
 * the frontend, as long as this mapping is updated to match.
 */
public record AlertResponse(
        Long id,
        Integer machineId,
        Double failureProbability,
        Integer cyclePosition,
        Instant receivedAt,
        String status
) {
    public static AlertResponse from(Alert alert) {
        return new AlertResponse(
                alert.getId(),
                alert.getMachineId(),
                alert.getFailureProbability(),
                alert.getCyclePosition(),
                alert.getReceivedAt(),
                alert.getStatus().name()
        );
    }
}
