"""LLM による情報抽出の補助（旧 ``text_prep.py`` の後半。ノートブック 4.2 の事業内容の抽出で使う）。

``groq`` は追加の依存（``pip install "edinet-xbrl-prep[llm]"``）。

旧版との違い:
- 応答が空のときに ``ValueError`` を作るだけで送出していなかった点を直した
- ``Prompt.instruction`` の説明（API Key と書かれていた）を直した
- 既定の temperature を 0 にした（旧版 0.6。同じ入力から同じ出力を得やすくするため）
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Literal

JSON_OBJECT = re.compile(r"{.*?}")


@dataclass
class Prompt:
    """指示・注意事項・回答形式・例からなるプロンプト。"""

    instruction: str
    example: str
    constraints_list: list[str]
    output_format: str

    def export(self, provided_text: str) -> tuple[str, str]:
        """（システムプロンプト, ユーザープロンプト）。"""
        constraint = "#### 注意事項\n * " + "\n * ".join(self.constraints_list)
        system_prompt = f"{self.instruction}\n\n{constraint}\n\n{self.output_format}\n\n#### 例\n{self.example}"
        user_prompt = f"#### 文章\n{provided_text}\n\n#### 回答\n"
        return system_prompt, user_prompt

    def export_sample(self, sample_text: str) -> None:
        system_prompt, user_prompt = self.export(sample_text)
        print("==== system_prompt ====")
        print(system_prompt)
        print("==== user_prompt ====")
        print(user_prompt)


def extract_json_objects(text: str) -> list[dict]:
    """応答から ``{...}`` の JSON を順に取り出す（入れ子の JSON は対象外）。"""
    if not text:
        raise ValueError("text is empty")
    return [json.loads(item) for item in JSON_OBJECT.findall(text.replace("\n", ""))]


@dataclass
class ResponseGenAI:
    output: str = ""
    input_token_size: int = 0
    output_token_size: int = 0
    status: Literal["success", "failure"] = "failure"
    error_message: str | None = None

    def output_json_validation(self) -> bool:
        try:
            self.extract_output_json()
        except (ValueError, json.JSONDecodeError):
            return False
        return True

    def extract_output_json(self) -> list[dict]:
        return extract_json_objects(self.output)


@dataclass
class GroqAPI:
    """Groq のチャット API。"""

    api_key: str
    max_retries: int = 2
    client: object = field(init=False, repr=False)

    def __post_init__(self) -> None:
        from groq import Groq

        self.client = Groq(api_key=self.api_key, max_retries=self.max_retries)

    def request(
        self,
        sys_prompt: str,
        usr_prompt: str,
        max_tokens: int = 1024,
        temperature: float = 0.0,
        model_name: str = "llama-3.1-8b-instant",
    ) -> ResponseGenAI:
        messages = [{"role": "system", "content": sys_prompt}, {"role": "user", "content": usr_prompt}]
        try:
            response = self.client.chat.completions.create(
                model=model_name, messages=messages, max_tokens=max_tokens, temperature=temperature
            )
        except Exception as exc:  # noqa: BLE001  API の失敗は結果に記録する
            return ResponseGenAI(error_message=str(exc).replace(self.api_key, "***"))
        return ResponseGenAI(
            output=response.choices[0].message.content or "",
            input_token_size=response.usage.prompt_tokens,
            output_token_size=response.usage.completion_tokens,
            status="success",
        )
