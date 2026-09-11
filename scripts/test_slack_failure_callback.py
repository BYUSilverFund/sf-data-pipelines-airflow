import sys
from pathlib import Path


sys.path.append(str(Path(__file__).resolve().parents[1] / "dags"))

from slack_notifier import slack_on_failure  # noqa: E402


class FakeTaskInstance:
    dag_id = "manual_test_dag"
    task_id = "manual_test_task"
    try_number = 1


class FakeDagRun:
    run_id = "manual__slack_test"


context = {
    "task_instance": FakeTaskInstance(),
    "dag_run": FakeDagRun(),
    "exception": ValueError("This is a test failure message"),
}

slack_on_failure(context)
