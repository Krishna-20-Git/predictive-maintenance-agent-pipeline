package com.krishna.orchestrator.entity;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * An alert produced by the Python streaming consumer (Day 7) and consumed
 * by this Spring Boot app (Day 9) from stream:scored-alerts. This is the
 * system-of-record copy of every alert that's crossed the probability
 * threshold — the React dashboard (Week 4) reads this table to render the
 * live triage board.
 */
@Entity
@Table(name = "alerts")
public class Alert {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "machine_id", nullable = false)
    private Integer machineId;

    @Column(name = "failure_probability", nullable = false)
    private Double failureProbability;

    @Column(name = "cycle_position")
    private Integer cyclePosition;

    @Column(name = "source_timestamp")
    private Double sourceTimestamp;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private AlertStatus status = AlertStatus.NEW;

    public enum AlertStatus {
        NEW,
        AGENT_TRIGGERED,
        ACKNOWLEDGED
    }

    protected Alert() {
        // required no-arg constructor for JPA
    }

    public Alert(Integer machineId, Double failureProbability, Integer cyclePosition, Double sourceTimestamp) {
        this.machineId = machineId;
        this.failureProbability = failureProbability;
        this.cyclePosition = cyclePosition;
        this.sourceTimestamp = sourceTimestamp;
        this.receivedAt = Instant.now();
        this.status = AlertStatus.NEW;
    }

    public Long getId() {
        return id;
    }

    public Integer getMachineId() {
        return machineId;
    }

    public Double getFailureProbability() {
        return failureProbability;
    }

    public Integer getCyclePosition() {
        return cyclePosition;
    }

    public Double getSourceTimestamp() {
        return sourceTimestamp;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public AlertStatus getStatus() {
        return status;
    }

    public void setStatus(AlertStatus status) {
        this.status = status;
    }
}
