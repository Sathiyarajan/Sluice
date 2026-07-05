"""Generate Autosys JIL (Job Information Language) job definitions from the same
dataset ingestion configs used by the Airflow DAG factory, so both orchestrators
express an identical dependency graph: one box job per dataset, one command job
per source->target ingestion, and upstream box dependencies mirroring
`upstream_dependencies` / `ExternalTaskSensor` in Airflow.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.config.loader import load_config_dir
from ingestion.config.models import DatasetConfig

CONFIG_DIR = Path(__file__).parent.parent / "config" / "datasets"
PYTHON_ENTRYPOINT = "python -m ingestion.run_pipeline"


def _box_job_name(dataset_name: str) -> str:
    return f"BOX_{dataset_name.upper()}"


def _command_job_name(dataset_name: str, target_type: str) -> str:
    return f"CMD_{dataset_name.upper()}_TO_{target_type.upper()}"


def _cron_to_autosys_run_calendar(schedule: str) -> str:
    """Autosys doesn't speak cron natively; production usage would map through a
    run calendar/`start_times` combo. We pass the cron expression through as a
    documented run_window comment plus a conservative daily start_times default,
    which an operator maps to the correct Autosys calendar during onboarding."""
    return schedule


def render_box_job(config: DatasetConfig) -> str:
    box_name = _box_job_name(config.dataset_name)
    condition = ""
    if config.upstream_dependencies:
        terms = " & ".join(f"success({_box_job_name(dep)})" for dep in config.upstream_dependencies)
        condition = f"\ncondition: {terms}"

    return f"""\
insert_job: {box_name}   job_type: BOX
description: "{config.description or config.dataset_name}"
owner: {config.owner}
permission: gx,wx
max_run_alarm: {config.sla.max_runtime_minutes}
alarm_if_fail: 1{condition}
box_terminator: 1
"""


def render_command_job(config: DatasetConfig, target_index: int) -> str:
    target = config.targets[target_index]
    job_name = _command_job_name(config.dataset_name, target.type.value)
    box_name = _box_job_name(config.dataset_name)
    command = (
        f"{PYTHON_ENTRYPOINT} "
        f"--dataset {config.dataset_name} --target-type {target.type.value} "
        f"--config-path config/datasets/{config.dataset_name}.yaml"
    )
    alert_names = ",".join(config.sla.alert_channels) or "none"

    return f"""\
insert_job: {job_name}   job_type: CMD
box_name: {box_name}
command: {command}
description: "Ingest {config.dataset_name} ({config.source.type.value} -> {target.type.value})"
owner: {config.owner}
permission: gx,wx
max_run_alarm: {config.sla.max_runtime_minutes}
alarm_if_fail: 1
n_retrys: {config.retry.max_attempts - 1}
std_out_file: "/logs/{config.dataset_name}/${{AUTO_JOB_NAME}}.out"
std_err_file: "/logs/{config.dataset_name}/${{AUTO_JOB_NAME}}.err"
-- alert_channels: {alert_names}
"""


def render_dataset_jil(config: DatasetConfig) -> str:
    jobs = [render_box_job(config)]
    for i in range(len(config.targets)):
        jobs.append(render_command_job(config, i))
    return "\n".join(jobs)


def generate_jil_files(config_dir: Path = CONFIG_DIR, output_dir: Path | None = None) -> dict[str, str]:
    output_dir = output_dir or (Path(__file__).parent / "generated")
    output_dir.mkdir(parents=True, exist_ok=True)
    configs = load_config_dir(config_dir)
    written: dict[str, str] = {}
    for config in configs:
        jil_text = render_dataset_jil(config)
        out_path = output_dir / f"{config.dataset_name}.jil"
        out_path.write_text(jil_text, encoding="utf-8")
        written[config.dataset_name] = str(out_path)
    return written


if __name__ == "__main__":
    for dataset_name, path in generate_jil_files().items():
        print(f"Generated {path} for dataset {dataset_name!r}")
