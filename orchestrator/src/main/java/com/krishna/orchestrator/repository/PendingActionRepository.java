package com.krishna.orchestrator.repository;

import com.krishna.orchestrator.entity.PendingAction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PendingActionRepository extends JpaRepository<PendingAction, Long> {

    /**
     * Used by the Day 19 approval queue endpoint — only ever shows actions
     * still awaiting a human decision.
     */
    List<PendingAction> findByStatus(PendingAction.ActionStatus status);
}
