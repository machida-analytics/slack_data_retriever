import datetime
import json
import os
from logging.config import dictConfig
from pathlib import Path
from pprint import pprint

import requests
from dotenv import load_dotenv

from slack_data_retriever.slack import SlackAPI


def setup_logging(log_file: Path):
    """
    ロギングの初期設定を行います
    """
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": "%(message)s",
            },
        },
        "handlers": {
            "file": {
                "class": "logging.FileHandler",
                "formatter": "json",
                "filename": log_file,
                "level": "INFO",
            },
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "json",
                "level": "WARNING",
            },
        },
        "root": {
            "handlers": ["file", "console"],
            "level": "INFO",
        },
    }

    dictConfig(config)


def main():
    # ロギングの初期設定
    now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log_file = Path("download", "metadata", "data_collection_logs", f"{now}.log")
    log_file.parent.mkdir(parents=True, exist_ok=True)
    setup_logging(log_file)

    # 環境変数読み込み
    load_dotenv()
    slack_token = os.environ.get("SLACK_APP_TOKEN")

    # ボットトークンとソケットモードハンドラーを使ってアプリを初期化します
    slack = SlackAPI(token=slack_token)
    download_dir = Path("download", "slack")
    # pprint(slack.list_channels())

    # チャンネル一覧を取得
    for channel in slack.list_channels():
        # Bot tokenの場合は下記のコメントアウトが必要
        # if not channel["is_member"]:
        #     continue

        # 会話履歴の取得
        # c.f. https://api.slack.com/methods/conversations.history
        messages = slack.list_messages(channel_id=channel["id"])

        # 会話履歴の保存
        save_path = download_dir / f'messages/{channel["id"]}.json'
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as fp:
            json.dump({"messages": messages}, fp, ensure_ascii=False, indent=2)

        for message in messages:
            # スレッドを取得
            if "thread_ts" in message:
                thread_ts = message["thread_ts"]
                thread = slack.list_thread_messages(
                    channel_id=channel["id"], thread_ts=thread_ts
                )

                # スレッドを保存
                save_path = download_dir / f'threads/{channel["id"]}/{thread_ts}.json'
                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, "w", encoding="utf-8") as fp:
                    json.dump({"thread": thread}, fp, ensure_ascii=False, indent=2)

            # 添付ファイルの取得
            # c.f. https://stackoverflow.com/questions/57015948/how-to-download-files-that-were-created-by-a-slackbot
            if "files" in message:
                for file_info in message["files"]:
                    file_name = file_info["name"]
                    file_url = file_info["url_private"]

                    # 添付ファイルのダウンロード
                    r = requests.get(
                        file_url, headers={"Authorization": f"Bearer {slack_token}"}
                    )
                    r.raise_for_status()

                    # 添付ファイルの保存
                    save_path = download_dir / "files" / file_name
                    save_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(save_path, "w+b") as fp:
                        fp.write(bytearray(r.content))


if __name__ == "__main__":
    main()
