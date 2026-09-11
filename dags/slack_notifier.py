import datetime as _dt
import logging
import os

import dotenv
import pytz
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

dotenv.load_dotenv(override=True)


logger = logging.getLogger(__name__)


def slack_on_failure(context: dict) -> None:
    """Send failure notification to Slack when a task fails.

    Uses `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` from environment.
    Context is automatically passed by Airflow on task failure.
    """
    token = os.getenv("SLACK_BOT_TOKEN")
    channel = os.getenv("SLACK_CHANNEL_ID")

    task_instance = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = getattr(dag_run, "run_id", "unknown")
    try_number = getattr(task_instance, "try_number", "unknown")
    error_message = repr(exception) if exception else "Unknown error"

    if not token or not channel:
        logger.warning(
            "SLACK_BOT_TOKEN or SLACK_CHANNEL_ID not set; skipping Slack notification"
        )
        return

    try:
        mst = pytz.timezone("America/Denver")
        time = _dt.datetime.now(tz=mst).strftime("%Y-%m-%d %H:%M:%S")

        # Format message
        text = (
            "❌ Airflow Task Failed\n"
            f"Time: {time} MST\n"
            f"DAG: {dag_id}\n"
            f"Run ID: {run_id}\n"
            f"Task: {task_id}\n"
            f"Attempt: {try_number}\n"
            f"Error: {error_message}"
        )

        client = WebClient(token=token)
        client.chat_postMessage(
            channel=channel, text=text, unfurl_links=False, unfurl_media=False
        )
        logger.info("Sent Slack failure notification to %s", channel)
    except SlackApiError as e:
        logger.exception("Failed to send Slack message: %s", getattr(e, "response", e))
    except Exception as e:
        logger.exception("Unexpected error in slack_on_failure: %s", str(e))
