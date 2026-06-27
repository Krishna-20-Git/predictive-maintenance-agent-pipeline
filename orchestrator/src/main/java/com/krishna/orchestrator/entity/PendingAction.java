package com.krishna.orchestrator.entity;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * A maintenance order proposed by the LangChain4j agent (Day 17-19), awaiting
 * human approval before any real action is taken. The agent NEVER writes
 * directly to production tables — it only ever inserts rows here. A human
 * must explicitly approve via the React dashboard (Day 24) before this
 * represents anything more than a recommendation.
 */
@Entity
@Table(name = "pending_actions")
public class PendingAction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "alert_id", nullable = false)
    private Long alertId;

    /**
     * The agent's structured output (Day 17's MaintenanceOrder), stored as
     * raw JSON text. Kept as a string rather than a parsed embedded object
     * so the schema doesn't need to change every time the agent's output
     * shape evolves — the frontend parses this JSON for display.
     */
    @Column(name = "maintenance_order_json", columnDefinition = "TEXT", nullable = false)
    private String maintenanceOrderJson;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ActionStatus status = ActionStatus.PENDING;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    public enum ActionStatus {
        PENDING,
        APPROVED,
        REJECTED
    }

    protected PendingAction() {
        // required no-arg constructor for JPA
    }

    public PendingAction(Long alertId, String maintenanceOrderJson) {
        this.alertId = alertId;
        this.maintenanceOrderJson = maintenanceOrderJson;
        this.status = ActionStatus.PENDING;
        this.createdAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public Long getAlertId() {
        return alertId;
    }

    public String getMaintenanceOrderJson() {
        return maintenanceOrderJson;
    }

    public ActionStatus getStatus() {
        return status;
    }

    public void setStatus(ActionStatus status) {
        this.status = status;
        this.resolvedAt = Instant.now();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getResolvedAt() {
        return resolvedAt;
    }
}
