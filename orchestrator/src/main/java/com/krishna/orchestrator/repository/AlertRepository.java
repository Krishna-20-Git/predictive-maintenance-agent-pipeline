package com.krishna.orchestrator.repository;

import com.krishna.orchestrator.entity.Alert;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AlertRepository extends JpaRepository<Alert, Long> {

    /**
     * Fetches the most recent alerts, newest first. Used by GET /api/alerts
     * to populate the live triage board — Pageable lets the caller control
     * how many rows come back (e.g. "last 50") without a hardcoded LIMIT.
     */
    List<Alert> findAllByOrderByReceivedAtDesc(Pageable pageable);
}
