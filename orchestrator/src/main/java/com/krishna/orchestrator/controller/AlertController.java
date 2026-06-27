package com.krishna.orchestrator.controller;

import com.krishna.orchestrator.dto.AlertResponse;
import com.krishna.orchestrator.entity.Alert;
import com.krishna.orchestrator.repository.AlertRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/alerts")
public class AlertController {

    private final AlertRepository alertRepository;

    public AlertController(AlertRepository alertRepository) {
        this.alertRepository = alertRepository;
    }

    /**
     * Returns the most recent alerts, newest first. Today (Day 8) this will
     * just return an empty list, since nothing is writing to the alerts
     * table yet — that's Day 9's job (the Redis Streams consumer). An empty
     * 200 OK response here is the correct, expected result for Day 8.
     */
    @GetMapping
    public List<AlertResponse> getRecentAlerts(
            @RequestParam(defaultValue = "50") int limit
    ) {
        List<Alert> alerts = alertRepository.findAllByOrderByReceivedAtDesc(PageRequest.of(0, limit));
        return alerts.stream().map(AlertResponse::from).toList();
    }
}
