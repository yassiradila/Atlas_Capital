"""
Telegram Alerting and Airflow Failure Callback.
Sends instant notifications on pipeline failures with complete context.
"""

import logging
import os
import requests

logger = logging.getLogger(__name__)


def send_telegram_alert(message: str) -> bool:
    """
    Sends a formatted message to the configured Telegram chat.
    If credentials are not set, logs the message locally.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id or "your_bot_token" in bot_token:
        logger.warning(
            "Telegram credentials not configured. Notification message:\n%s",
            message,
        )
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Successfully sent Telegram alert.")
        return True
    except Exception as e:
        logger.error("Failed to send Telegram alert: %s", e)
        return False


def airflow_failure_callback(context: dict) -> None:
    """
    Airflow task failure callback.
    Automatically invoked by Airflow whenever any task fails.
    """
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else context.get("dag", {}).dag_id
    task_id = ti.task_id if ti else "unknown_task"
    logical_date = context.get("ds") or str(context.get("logical_date", "unknown"))
    exception = context.get("exception", "No exception message provided")
    log_url = ti.log_url if ti else "N/A"

    alert_msg = (
        f"🚨 *Airflow Task Failure Alert*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Logical Date*: `{logical_date}`\n"
        f"*Error*: `{str(exception)[:200]}`\n"
        f"*Logs*: [View Logs]({log_url})\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

    send_telegram_alert(alert_msg)
