# Real-time anomaly detection & agentic maintenance pipeline

> 🚧 Work in progress — building day by day. See `docs/` as it fills in.

An event-driven MLOps pipeline that scores streaming IoT sensor data for
equipment failure risk, then hands high-risk cases to a local LLM agent
that drafts maintenance orders for human approval.

**Stack:** Python (FastAPI, XGBoost) · Redis Streams (Upstash) · Java
(Spring Boot, LangChain4j) · PostgreSQL (+ pgvector) · Ollama · React

## Status

- [x] Day 1 — Project scaffold & environment setup
- [ ] Day 2 — Dataset & EDA
- [ ] ... (28-day build log to come)

## Quick start

```bash
cp .env.example .env   # fill in your Upstash Redis credentials
docker compose up
```

(Full instructions land in Week 4 once the stack is complete.)
