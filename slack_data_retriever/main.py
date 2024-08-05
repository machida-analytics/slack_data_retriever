import json
import os
from pathlib import Path
from pprint import pprint

import requests
from dotenv import load_dotenv

from slack_data_retriever.slack import SlackAPI


def main():
    download_dir = Path("download")

    # 環境変数読み込み
    load_dotenv()
    slack_token = os.environ.get("SLACK_BOT_TOKEN")

    # ボットトークンとソケットモードハンドラーを使ってアプリを初期化します
    slack = SlackAPI(token=slack_token)
    # pprint(slack.list_channels())

    # チャンネル一覧を取得
    for channel in slack.list_channels():
        if not channel["is_member"]:
            continue

        # 会話履歴の取得
        # c.f. https://api.slack.com/methods/conversations.history
        messages = slack.list_messages(channel_id=channel["id"])

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

            # Download attached files
            # c.f. https://stackoverflow.com/questions/57015948/how-to-download-files-that-were-created-by-a-slackbot
            if "files" in message:
                for file_info in message["files"]:
                    file_name = file_info["name"]
                    file_url = file_info["url_private"]

                    # 添付ファイルのダウンロード
                    print(f"Downloading {file_name}...")
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
