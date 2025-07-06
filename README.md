# lightweight-etl-pipeline-to-gcp


Python-based ETL pipeline orchestrator designed for environments where Airflow is unavailable (due to permissions, infrastructure constraints, or complexity). It provides a no-frills, dependency-light way to define, schedule, and monitor ETL workflows using Python libraries.

Objectives
- Works in Restricted Environments – No need for Airflow, Docker, or complex setups.
- Easy to Deploy – Runs anywhere Python runs (even on locked-down servers).
- Minimal Overhead – Perfect for small scale ETL needs.

Features
- Task Dependencies – Simple depends_on syntax for execution order.
- Basic Retry Logic – Automatic retries for failed tasks.
- Logging & Failure Alerts – Log to file and send email/Slack alerts (optional).
- Flexible Scheduling – Works with schedule, APScheduler, or cron.

Project Structure


Setup
- Installation....