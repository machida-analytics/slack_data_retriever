import logging
from typing import Any, Dict, List

from slack_bolt import App


class SlackAPI:
    """
    SlackのAPIを抽象化したクラス

    このクラスは、Slack APIの主要な機能をラップし、
    チャンネルやメッセージの取得を簡単に行えるようにします。

    Args:
        token (str): Slack APIのアクセストークン
    """

    def __init__(self, token: str):
        self.app = App(token=token)

    def _paginate_request(self, method: str, **kwargs) -> List[Dict[str, Any]]:
        """
        ページネーション対応のAPIリクエストを行う内部メソッド

        Args:
            method (str): 呼び出すSlack APIメソッド
            **kwargs: APIメソッドに渡す追加のパラメータ

        Returns:
            List[Dict[str, Any]]: 全ページの結果を結合したリスト
        """
        all_results = []
        cursor = None
        while True:
            if cursor:
                kwargs["cursor"] = cursor
            response = self.app.client.api_call(api_method=method, params=kwargs)

            # okフィールドがFalseの場合は例外を発生させる
            if not response.get("ok", False):
                raise Exception("Failed to get messages")

            # チャンネルまたはメッセージの結果を追加
            all_results.extend(
                response.get("channels", []) or response.get("messages", [])
            )

            # 次のページがあれば、cursorを更新
            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

        return all_results

    def list_channels(
        self, types: str = "public_channel,private_channel"
    ) -> List[Dict[str, Any]]:
        """
        公開チャンネルと非公開チャンネルの一覧を取得

        Returns:
            List[Dict[str, Any]]: チャンネル情報のリスト
        """
        logging.info("Fetching channel list.")
        return self._paginate_request("conversations.list", types=types)

    def list_messages(self, channel_id: str) -> List[Dict[str, Any]]:
        """
        指定されたチャンネル内のメッセージ一覧を取得

        Args:
            channel_id (str): メッセージを取得するチャンネルのID

        Returns:
            List[Dict[str, Any]]: メッセージ情報のリスト
        """
        logging.info(
            f"Fetching messages in {channel_id}.", extra={"channel_id": channel_id}
        )
        return self._paginate_request("conversations.history", channel=channel_id)[::-1]

    def list_thread_messages(
        self, channel_id: str, thread_ts: str
    ) -> List[Dict[str, Any]]:
        """
        指定されたスレッド内のメッセージ一覧を取得

        Args:
            channel_id (str): スレッドが存在するチャンネルのID
            thread_ts (str): スレッドの親メッセージのタイムスタンプ

        Returns:
            List[Dict[str, Any]]: スレッド内のメッセージ情報のリスト
        """
        logging.info(
            f"Fetching thread messages in {channel_id}.",
            extra={"channel_id": channel_id, "thread_ts": thread_ts},
        )
        return self._paginate_request(
            "conversations.replies", channel=channel_id, ts=thread_ts
        )[1:]

    def team_info(self) -> Dict[str, Any]:
        """
        チーム情報を取得

        Returns:
            Dict[str, Any]: チーム情報
        """
        logging.info("Fetching team info.")
        return self._paginate_request("team.info")
