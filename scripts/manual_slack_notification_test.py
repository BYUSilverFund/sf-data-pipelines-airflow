import json
import os
import shlex
import sys
import urllib.error
import urllib.request
from pathlib import Path


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        values = shlex.split(value, comments=True, posix=True)
        value = values[0] if values else ""
        os.environ[key] = value


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    load_env_file(repo_root / ".env")

    token = os.getenv("SLACK_BOT_TOKEN")
    channel = os.getenv("SLACK_CHANNEL_ID")

    if not token or not channel:
        print("Missing SLACK_BOT_TOKEN or SLACK_CHANNEL_ID in .env")
        return 1

    print(f"Using Slack channel: {channel}")

    payload = {
        "channel": channel,
        "text": "test",
        "unfurl_links": False,
        "unfurl_media": False,
    }
    request = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        print(f"Could not reach Slack API: {exc}")
        return 1

    if not result.get("ok"):
        error = result.get("error", result)
        print(f"Slack API error: {error}")
        if error == "channel_not_found":
            print("Fixes to try:")
            print(
                "- Use the channel ID, not the channel name. It usually starts with C or G."
            )
            print("- Invite the Slack bot to the channel with: /invite @YourAppName")
            print("- For private channels, the bot must be explicitly invited.")
            print(
                "- Confirm SLACK_BOT_TOKEN belongs to the same workspace as the channel."
            )
        return 1

    print(f"Sent Slack test message to {channel}: {result['ts']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
