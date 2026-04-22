#!/usr/bin/env python3
from __future__ import annotations

import queue
import subprocess
import sys
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List

import boto3
import tkinter as tk
from botocore.config import Config
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText


ENV_KEY_ORDER: List[str] = [
    "SRC_BUCKET",
    "DST_BUCKET",
    "SRC_REGION",
    "DST_REGION",
    "SRC_S3_ENDPOINT",
    "DST_S3_ENDPOINT",
    "SRC_S3_ADDRESSING_STYLE",
    "DST_S3_ADDRESSING_STYLE",
    "PREFIX",
    "EXACT_KEY",
    "SRC_AWS_ACCESS_KEY_ID",
    "SRC_AWS_SECRET_ACCESS_KEY",
    "SRC_AWS_SESSION_TOKEN",
    "SRC_AWS_PROFILE",
    "DST_AWS_ACCESS_KEY_ID",
    "DST_AWS_SECRET_ACCESS_KEY",
    "DST_AWS_SESSION_TOKEN",
    "DST_AWS_PROFILE",
    "WORKERS",
    "MULTIPART_THRESHOLD_MB",
    "MULTIPART_CHUNK_MB",
    "PROGRESS_EVERY",
    "LOG_LEVEL",
    "LOG_FILE",
    "FULL_RESPONSE",
    "DEBUG_BOTOCORE",
    "TRACEBACK_ON_ERROR",
]

DEFAULT_ENV: Dict[str, str] = {
    "SRC_BUCKET": "",
    "DST_BUCKET": "",
    "SRC_REGION": "",
    "DST_REGION": "",
    "SRC_S3_ENDPOINT": "",
    "DST_S3_ENDPOINT": "",
    "SRC_S3_ADDRESSING_STYLE": "auto",
    "DST_S3_ADDRESSING_STYLE": "auto",
    "PREFIX": "",
    "EXACT_KEY": "",
    "SRC_AWS_ACCESS_KEY_ID": "",
    "SRC_AWS_SECRET_ACCESS_KEY": "",
    "SRC_AWS_SESSION_TOKEN": "",
    "SRC_AWS_PROFILE": "",
    "DST_AWS_ACCESS_KEY_ID": "",
    "DST_AWS_SECRET_ACCESS_KEY": "",
    "DST_AWS_SESSION_TOKEN": "",
    "DST_AWS_PROFILE": "",
    "WORKERS": "8",
    "MULTIPART_THRESHOLD_MB": "64",
    "MULTIPART_CHUNK_MB": "16",
    "PROGRESS_EVERY": "200",
    "LOG_LEVEL": "INFO",
    "LOG_FILE": "",
    "FULL_RESPONSE": "false",
    "DEBUG_BOTOCORE": "false",
    "TRACEBACK_ON_ERROR": "false",
}

ERROR_PATTERNS = (
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
    "AccessDenied",
    "ERROR",
    "Traceback",
    "FAILED",
)


def env_to_bool(text: str) -> bool:
    value = (text or "").strip().lower()
    return value in {"1", "true", "t", "yes", "y", "on"}


def has_text(value: str | None) -> bool:
    return value is not None and value.strip() != ""


def build_client_config(addressing_style: str) -> Config:
    if addressing_style in {"path", "virtual"}:
        return Config(retries={"max_attempts": 8, "mode": "adaptive"}, s3={"addressing_style": addressing_style})
    return Config(retries={"max_attempts": 8, "mode": "adaptive"})


def human_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    if size < 1024 * 1024 * 1024:
        return f"{size / (1024 * 1024):.1f} MB"
    return f"{size / (1024 * 1024 * 1024):.2f} GB"


def parse_env_file(path: Path) -> OrderedDict[str, str]:
    data: OrderedDict[str, str] = OrderedDict()
    if not path.exists():
        for key in ENV_KEY_ORDER:
            data[key] = DEFAULT_ENV.get(key, "")
        return data

    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        data[key] = value

    for key in ENV_KEY_ORDER:
        data.setdefault(key, DEFAULT_ENV.get(key, ""))
    return data


def write_env_file(path: Path, data: Dict[str, str]) -> None:
    lines = [
        "# S3 Sync GUI generated .env",
        "# UTF-8",
        "",
    ]
    for key in ENV_KEY_ORDER:
        value = data.get(key, "")
        lines.append(f"{key}={value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def mask_ak(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return "-"
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}...{text[-4:]}"


class SyncGuiApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("S3 同期ツール (Python/Tkinter)")
        self.root.geometry("1280x860")
        self.root.minsize(1120, 760)

        self.env_file_var = tk.StringVar(value=str(Path.cwd() / ".env"))
        self.scope_var = tk.StringVar(value="all")
        self.safety_mode_var = tk.BooleanVar(value=True)
        self.dry_run_var = tk.BooleanVar(value=True)
        self.delete_var = tk.BooleanVar(value=False)
        self.copy_tags_var = tk.BooleanVar(value=False)
        self.force_var = tk.BooleanVar(value=False)
        self.full_response_var = tk.BooleanVar(value=False)
        self.debug_botocore_var = tk.BooleanVar(value=False)
        self.traceback_var = tk.BooleanVar(value=False)
        self.log_level_var = tk.StringVar(value="INFO")

        self.env_vars: Dict[str, tk.StringVar] = {}
        for key, val in DEFAULT_ENV.items():
            self.env_vars[key] = tk.StringVar(value=val)

        self.browser: Dict[str, Dict[str, Any]] = {
            "SRC": {"tree": None, "meta": {}, "status_var": tk.StringVar(value="未読込"), "loading": False},
            "DST": {"tree": None, "meta": {}, "status_var": tk.StringVar(value="未読込"), "loading": False},
        }
        self.process: subprocess.Popen[str] | None = None
        self.log_queue: queue.Queue[str] = queue.Queue()

        self._build_ui()
        self.load_env()
        self._refresh_scope_state()
        self._apply_safety_mode()
        self.root.after(150, self._drain_log_queue)

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=10)
        main.pack(fill=tk.BOTH, expand=True)

        top = ttk.LabelFrame(main, text="設定ファイル", padding=8)
        top.pack(fill=tk.X)

        ttk.Label(top, text=".env パス").grid(row=0, column=0, sticky=tk.W, padx=4, pady=4)
        ttk.Entry(top, textvariable=self.env_file_var, width=84).grid(row=0, column=1, sticky=tk.EW, padx=4, pady=4)
        ttk.Button(top, text="開く...", command=self.pick_env_file).grid(row=0, column=2, padx=4, pady=4)
        ttk.Button(top, text="読込", command=self.load_env).grid(row=0, column=3, padx=4, pady=4)
        ttk.Button(top, text="保存", command=self.save_env).grid(row=0, column=4, padx=4, pady=4)
        top.columnconfigure(1, weight=1)

        paned = ttk.Panedwindow(main, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

        left = ttk.Frame(paned)
        right = ttk.Frame(paned)
        paned.add(left, weight=3)
        paned.add(right, weight=2)

        self._build_left(left)
        self._build_right(right)

    def _build_left(self, parent: ttk.Frame) -> None:
        cred = ttk.LabelFrame(parent, text="S3 接続情報", padding=8)
        cred.pack(fill=tk.BOTH, expand=False)

        src = ttk.LabelFrame(cred, text="ソース (生産側)", padding=8)
        src.grid(row=0, column=0, sticky=tk.NSEW, padx=(0, 6), pady=4)
        dst = ttk.LabelFrame(cred, text="ターゲット (テスト側)", padding=8)
        dst.grid(row=0, column=1, sticky=tk.NSEW, padx=(6, 0), pady=4)
        cred.columnconfigure(0, weight=1)
        cred.columnconfigure(1, weight=1)

        self._build_side_form(src, "SRC")
        self._build_side_form(dst, "DST")

        mode = ttk.LabelFrame(parent, text="同期モード", padding=8)
        mode.pack(fill=tk.X, pady=8)
        self.rb_all = ttk.Radiobutton(mode, text="バケット全体を同期", variable=self.scope_var, value="all", command=self._refresh_scope_state)
        self.rb_prefix = ttk.Radiobutton(mode, text="フォルダ単位で同期", variable=self.scope_var, value="prefix", command=self._refresh_scope_state)
        self.rb_exact = ttk.Radiobutton(mode, text="ファイル単位で同期", variable=self.scope_var, value="exact", command=self._refresh_scope_state)
        self.rb_all.grid(row=0, column=0, sticky=tk.W, padx=4, pady=4)
        self.rb_prefix.grid(row=0, column=1, sticky=tk.W, padx=4, pady=4)
        self.rb_exact.grid(row=0, column=2, sticky=tk.W, padx=4, pady=4)

        ttk.Label(mode, text="フォルダパス").grid(row=1, column=0, sticky=tk.W, padx=4, pady=4)
        self.prefix_entry = ttk.Entry(mode, textvariable=self.env_vars["PREFIX"], width=60)
        self.prefix_entry.grid(row=1, column=1, columnspan=2, sticky=tk.EW, padx=4, pady=4)

        ttk.Label(mode, text="ファイルパス").grid(row=2, column=0, sticky=tk.W, padx=4, pady=4)
        self.exact_entry = ttk.Entry(mode, textvariable=self.env_vars["EXACT_KEY"], width=60)
        self.exact_entry.grid(row=2, column=1, columnspan=2, sticky=tk.EW, padx=4, pady=4)
        mode.columnconfigure(1, weight=1)

        opt = ttk.LabelFrame(parent, text="実行オプション", padding=8)
        opt.pack(fill=tk.X, pady=8)
        self.option_area = ttk.Frame(opt)
        self.option_area.pack(fill=tk.X, expand=True)

        self.option_widgets: list[ttk.Checkbutton] = []
        self.cb_safety = ttk.Checkbutton(
            self.option_area,
            text="安全運転モード（常に事前確認のみ）",
            variable=self.safety_mode_var,
            command=self._apply_safety_mode,
        )
        self.option_widgets.append(self.cb_safety)
        self.dry_run_cb = ttk.Checkbutton(
            self.option_area,
            text="事前確認のみ（変更しない）",
            variable=self.dry_run_var,
        )
        self.option_widgets.append(self.dry_run_cb)
        self.cb_delete = ttk.Checkbutton(
            self.option_area,
            text="余分ファイル削除（完全同期）",
            variable=self.delete_var,
        )
        self.option_widgets.append(self.cb_delete)
        self.cb_copy_tags = ttk.Checkbutton(
            self.option_area,
            text="オブジェクトタグもコピー",
            variable=self.copy_tags_var,
        )
        self.option_widgets.append(self.cb_copy_tags)
        self.cb_force = ttk.Checkbutton(
            self.option_area,
            text="常に上書き",
            variable=self.force_var,
        )
        self.option_widgets.append(self.cb_force)
        self.cb_full_response = ttk.Checkbutton(
            self.option_area,
            text="詳細APIレスポンスを表示",
            variable=self.full_response_var,
        )
        self.option_widgets.append(self.cb_full_response)
        self.cb_debug_botocore = ttk.Checkbutton(
            self.option_area,
            text="通信デバッグログを表示",
            variable=self.debug_botocore_var,
        )
        self.option_widgets.append(self.cb_debug_botocore)
        self.cb_traceback = ttk.Checkbutton(
            self.option_area,
            text="例外スタックトレース表示",
            variable=self.traceback_var,
        )
        self.option_widgets.append(self.cb_traceback)
        self.option_area.bind("<Configure>", lambda _e: self._layout_option_controls())

        perf = ttk.LabelFrame(parent, text="転送パラメータ", padding=8)
        perf.pack(fill=tk.X, pady=8)
        self.parameter_area = ttk.Frame(perf)
        self.parameter_area.pack(fill=tk.X, expand=True)
        self.parameter_items: list[ttk.Frame] = []
        self.parameter_items.append(self._create_parameter_item(self.parameter_area, "同時転送数", self.env_vars["WORKERS"]))
        self.parameter_items.append(
            self._create_parameter_item(self.parameter_area, "分割転送開始サイズ (MB)", self.env_vars["MULTIPART_THRESHOLD_MB"])
        )
        self.parameter_items.append(
            self._create_parameter_item(self.parameter_area, "分割チャンクサイズ (MB)", self.env_vars["MULTIPART_CHUNK_MB"])
        )
        self.parameter_items.append(self._create_parameter_item(self.parameter_area, "進捗表示間隔 (件)", self.env_vars["PROGRESS_EVERY"]))

        level_item = ttk.Frame(self.parameter_area, padding=(4, 4))
        ttk.Label(level_item, text="ログレベル").pack(anchor=tk.W)
        ttk.Combobox(
            level_item,
            textvariable=self.log_level_var,
            values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            state="readonly",
            width=22,
        ).pack(fill=tk.X)
        self.parameter_items.append(level_item)

        self.parameter_items.append(self._create_parameter_item(self.parameter_area, "ログ保存先", self.env_vars["LOG_FILE"]))
        self.parameter_area.bind("<Configure>", lambda _e: self._layout_parameter_controls())
        self.root.after_idle(self._layout_option_controls)
        self.root.after_idle(self._layout_parameter_controls)

    def _build_right(self, parent: ttk.Frame) -> None:
        test_frame = ttk.LabelFrame(parent, text="認証 / 接続テスト", padding=8)
        test_frame.pack(fill=tk.X)
        ttk.Button(test_frame, text="ソーステスト", command=lambda: self.run_test("src")).grid(row=0, column=0, sticky=tk.EW, padx=4, pady=4)
        ttk.Button(test_frame, text="ターゲットテスト", command=lambda: self.run_test("dst")).grid(row=0, column=1, sticky=tk.EW, padx=4, pady=4)
        ttk.Button(test_frame, text="両方テスト", command=lambda: self.run_test("both")).grid(row=0, column=2, sticky=tk.EW, padx=4, pady=4)
        test_frame.columnconfigure(0, weight=1)
        test_frame.columnconfigure(1, weight=1)
        test_frame.columnconfigure(2, weight=1)

        summary = ttk.LabelFrame(parent, text="接続サマリー", padding=8)
        summary.pack(fill=tk.X, pady=8)
        self.summary_var = tk.StringVar(value="")
        ttk.Label(summary, textvariable=self.summary_var, justify=tk.LEFT).pack(anchor=tk.W)

        browser = ttk.LabelFrame(parent, text="バケットファイルブラウザ", padding=8)
        browser.pack(fill=tk.BOTH, expand=True, pady=(0, 8))
        notebook = ttk.Notebook(browser)
        notebook.pack(fill=tk.BOTH, expand=True)
        src_tab = ttk.Frame(notebook)
        dst_tab = ttk.Frame(notebook)
        notebook.add(src_tab, text="ソース")
        notebook.add(dst_tab, text="ターゲット")
        self._build_browser_tab(src_tab, "SRC")
        self._build_browser_tab(dst_tab, "DST")

        run = ttk.LabelFrame(parent, text="同期実行", padding=8)
        run.pack(fill=tk.X)
        ttk.Button(run, text="同期開始", command=self.run_sync).grid(row=0, column=0, sticky=tk.EW, padx=4, pady=4)
        ttk.Button(run, text="停止", command=self.stop_process).grid(row=0, column=1, sticky=tk.EW, padx=4, pady=4)
        ttk.Button(run, text="ログクリア", command=self.clear_log).grid(row=0, column=2, sticky=tk.EW, padx=4, pady=4)
        run.columnconfigure(0, weight=1)
        run.columnconfigure(1, weight=1)
        run.columnconfigure(2, weight=1)

        log_box = ttk.LabelFrame(parent, text="ログ", padding=8)
        log_box.pack(fill=tk.BOTH, expand=True, pady=8)
        self.log_text = ScrolledText(log_box, wrap=tk.WORD, height=22, font=("Consolas", 10))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.tag_config("error", foreground="#b00020")
        self.log_text.tag_config("warn", foreground="#996300")
        self.log_text.tag_config("ok", foreground="#006400")

    def _build_browser_tab(self, parent: ttk.Frame, side: str) -> None:
        control = ttk.Frame(parent)
        control.pack(fill=tk.X, pady=(0, 4))
        side_name = "ソース" if side == "SRC" else "ターゲット"
        ttk.Button(control, text="ルート読込", command=lambda s=side: self.load_browser_root(s)).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(control, text="選択を同期対象に設定", command=lambda s=side: self.apply_browser_selection(s)).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Label(control, text=f"{side_name} バケット").pack(side=tk.LEFT, padx=(10, 4))

        ttk.Label(parent, textvariable=self.browser[side]["status_var"]).pack(anchor=tk.W, pady=(0, 4))

        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        tree = ttk.Treeview(tree_frame, columns=("kind", "size"), show="tree headings")
        tree.heading("#0", text="名前")
        tree.heading("kind", text="種別")
        tree.heading("size", text="サイズ")
        tree.column("#0", width=290, stretch=True)
        tree.column("kind", width=70, stretch=False, anchor=tk.CENTER)
        tree.column("size", width=120, stretch=False, anchor=tk.E)
        y_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=y_scroll.set)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        tree.bind("<<TreeviewOpen>>", lambda e, s=side: self.on_browser_open(s))
        tree.bind("<Double-1>", lambda e, s=side: self.apply_browser_selection(s))
        self.browser[side]["tree"] = tree

    def _build_side_form(self, frame: ttk.LabelFrame, prefix: str) -> None:
        fields = [
            ("バケット名", "BUCKET", 42),
            ("リージョン", "REGION", 24),
            ("S3 エンドポイント", "S3_ENDPOINT", 42),
            ("アドレッシング方式", "S3_ADDRESSING_STYLE", 24),
            ("アクセスキー", "AWS_ACCESS_KEY_ID", 42),
            ("シークレットキー", "AWS_SECRET_ACCESS_KEY", 42),
            ("セッショントークン", "AWS_SESSION_TOKEN", 42),
            ("AWS プロファイル", "AWS_PROFILE", 24),
        ]
        for idx, (label, suffix, width) in enumerate(fields):
            key = f"{prefix}_{suffix}"
            ttk.Label(frame, text=label).grid(row=idx, column=0, sticky=tk.W, padx=4, pady=3)
            if suffix == "S3_ADDRESSING_STYLE":
                ttk.Combobox(
                    frame,
                    textvariable=self.env_vars[key],
                    values=["auto", "path", "virtual"],
                    state="readonly",
                    width=14,
                ).grid(row=idx, column=1, sticky=tk.W, padx=4, pady=3)
            else:
                show = "*" if suffix == "AWS_SECRET_ACCESS_KEY" else ""
                ttk.Entry(frame, textvariable=self.env_vars[key], width=width, show=show).grid(
                    row=idx, column=1, sticky=tk.EW, padx=4, pady=3
                )
        frame.columnconfigure(1, weight=1)

    def _create_parameter_item(self, parent: ttk.Frame, label: str, text_var: tk.StringVar) -> ttk.Frame:
        item = ttk.Frame(parent, padding=(4, 4))
        ttk.Label(item, text=label).pack(anchor=tk.W)
        ttk.Entry(item, textvariable=text_var).pack(fill=tk.X)
        return item

    def _layout_flow_widgets(self, container: ttk.Frame, widgets: list[tk.Widget], *, min_cell_width: int, pad: int = 4) -> None:
        if not widgets:
            return
        width = max(container.winfo_width(), 1)
        cols = max(1, width // min_cell_width)
        for w in widgets:
            w.grid_forget()
        for col in range(12):
            container.columnconfigure(col, weight=0)
        for col in range(cols):
            container.columnconfigure(col, weight=1)
        for idx, widget in enumerate(widgets):
            row = idx // cols
            col = idx % cols
            widget.grid(row=row, column=col, sticky=tk.W + tk.E, padx=pad, pady=pad)

    def _layout_option_controls(self) -> None:
        self._layout_flow_widgets(self.option_area, self.option_widgets, min_cell_width=320, pad=4)

    def _layout_parameter_controls(self) -> None:
        self._layout_flow_widgets(self.parameter_area, self.parameter_items, min_cell_width=280, pad=4)

    def _ensure_browser_not_busy(self, side: str) -> bool:
        if self.browser[side]["loading"]:
            self._append_log(f"[WARNING] {side} ブラウザは読込中です。完了を待ってください。")
            return False
        return True

    def _build_s3_client_for_side(self, side: str):
        endpoint = self.env_vars[f"{side}_S3_ENDPOINT"].get().strip() or None
        region = self.env_vars[f"{side}_REGION"].get().strip() or None
        addressing_style = self.env_vars[f"{side}_S3_ADDRESSING_STYLE"].get().strip() or "auto"
        ak = self.env_vars[f"{side}_AWS_ACCESS_KEY_ID"].get().strip() or None
        sk = self.env_vars[f"{side}_AWS_SECRET_ACCESS_KEY"].get().strip() or None
        token = self.env_vars[f"{side}_AWS_SESSION_TOKEN"].get().strip() or None
        profile = self.env_vars[f"{side}_AWS_PROFILE"].get().strip() or None

        resolved_region = region or ("us-east-1" if endpoint else None)
        client_kwargs: Dict[str, Any] = {"config": build_client_config(addressing_style)}
        if endpoint:
            client_kwargs["endpoint_url"] = endpoint

        if has_text(ak) and has_text(sk):
            session = boto3.Session(
                aws_access_key_id=ak,
                aws_secret_access_key=sk,
                aws_session_token=token,
                region_name=resolved_region,
            )
        elif profile:
            session = boto3.Session(profile_name=profile, region_name=resolved_region)
        else:
            session = boto3.Session(region_name=resolved_region)
        return session.client("s3", **client_kwargs)

    def _list_one_level(self, side: str, prefix: str) -> tuple[list[str], list[dict[str, Any]]]:
        bucket = self.env_vars[f"{side}_BUCKET"].get().strip()
        if not bucket:
            raise ValueError(f"{side}_BUCKET が未入力です。")
        client = self._build_s3_client_for_side(side)

        folders: set[str] = set()
        files: list[dict[str, Any]] = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                p = cp.get("Prefix", "")
                if p:
                    folders.add(p)
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                if not key or key == prefix:
                    continue
                if key.endswith("/") and int(obj.get("Size", 0)) == 0:
                    folders.add(key)
                    continue
                files.append({"key": key, "size": int(obj.get("Size", 0))})
        files.sort(key=lambda x: x["key"])
        return sorted(folders), files

    def _reset_node_with_placeholder(self, side: str, node_id: str) -> None:
        tree: ttk.Treeview = self.browser[side]["tree"]
        for child in tree.get_children(node_id):
            tree.delete(child)
        meta = self.browser[side]["meta"]
        ph = tree.insert(node_id, tk.END, text="読み込み中...", values=("...", ""))
        meta[ph] = {"type": "placeholder"}

    def _populate_node(self, side: str, node_id: str, prefix: str, folders: list[str], files: list[dict[str, Any]]) -> None:
        tree: ttk.Treeview = self.browser[side]["tree"]
        meta = self.browser[side]["meta"]
        for child in tree.get_children(node_id):
            tree.delete(child)

        for folder_prefix in folders:
            name = folder_prefix[len(prefix) :].rstrip("/") if folder_prefix.startswith(prefix) else folder_prefix.rstrip("/")
            if not name:
                name = folder_prefix.rstrip("/")
            child = tree.insert(node_id, tk.END, text=name, values=("フォルダ", ""))
            meta[child] = {"type": "folder", "path": folder_prefix, "loaded": False}
            dummy = tree.insert(child, tk.END, text="展開して読込", values=("", ""))
            meta[dummy] = {"type": "placeholder"}

        for item in files:
            key = item["key"]
            name = key[len(prefix) :] if key.startswith(prefix) else key
            child = tree.insert(node_id, tk.END, text=name, values=("ファイル", human_size(item["size"])))
            meta[child] = {"type": "file", "path": key, "size": item["size"]}

        if not folders and not files:
            empty = tree.insert(node_id, tk.END, text="(空)", values=("", ""))
            meta[empty] = {"type": "placeholder"}

        if node_id in meta and meta[node_id].get("type") in {"folder", "bucket"}:
            meta[node_id]["loaded"] = True

    def _load_node_async(self, side: str, node_id: str, prefix: str) -> None:
        if not self._ensure_browser_not_busy(side):
            return

        tree: ttk.Treeview = self.browser[side]["tree"]
        if tree is None:
            return

        self.browser[side]["loading"] = True
        self._reset_node_with_placeholder(side, node_id)
        bucket = self.env_vars[f"{side}_BUCKET"].get().strip() or "<未設定>"
        self.browser[side]["status_var"].set(f"読込中: {bucket} / {prefix or '/'}")

        def worker() -> None:
            try:
                folders, files = self._list_one_level(side, prefix)
            except Exception as exc:
                self.root.after(0, lambda: self._on_browser_error(side, exc))
                return
            self.root.after(0, lambda: self._on_browser_loaded(side, node_id, prefix, folders, files))

        threading.Thread(target=worker, daemon=True).start()

    def _on_browser_loaded(self, side: str, node_id: str, prefix: str, folders: list[str], files: list[dict[str, Any]]) -> None:
        self.browser[side]["loading"] = False
        self._populate_node(side, node_id, prefix, folders, files)
        bucket = self.env_vars[f"{side}_BUCKET"].get().strip() or "<未設定>"
        self.browser[side]["status_var"].set(
            f"読込完了: {bucket} / {prefix or '/'} (フォルダ={len(folders)} ファイル={len(files)})"
        )

    def _on_browser_error(self, side: str, exc: Exception) -> None:
        self.browser[side]["loading"] = False
        self.browser[side]["status_var"].set(f"読込失敗: {exc}")
        self._append_log(f"[ERROR] {side} 一覧取得失敗: {exc}")

    def load_browser_root(self, side: str) -> None:
        tree: ttk.Treeview = self.browser[side]["tree"]
        if tree is None:
            return
        bucket = self.env_vars[f"{side}_BUCKET"].get().strip()
        if not bucket:
            messagebox.showerror("入力エラー", f"{side}_BUCKET が未入力です。")
            return

        self.browser[side]["meta"].clear()
        for node in tree.get_children():
            tree.delete(node)
        root_id = tree.insert("", tk.END, text=bucket, values=("バケット", ""), open=True)
        self.browser[side]["meta"][root_id] = {"type": "bucket", "path": "", "loaded": False}
        self._load_node_async(side, root_id, "")
        self._append_log(f"[INFO] {side} バケット一覧読込開始: {bucket}")

    def on_browser_open(self, side: str) -> None:
        tree: ttk.Treeview = self.browser[side]["tree"]
        if tree is None:
            return
        node_id = tree.focus()
        if not node_id:
            return
        meta = self.browser[side]["meta"].get(node_id)
        if not meta:
            return
        if meta.get("type") not in {"folder", "bucket"}:
            return
        if meta.get("loaded"):
            return
        self._load_node_async(side, node_id, meta.get("path", ""))

    def apply_browser_selection(self, side: str) -> None:
        tree: ttk.Treeview = self.browser[side]["tree"]
        if tree is None:
            return
        selected = tree.selection()
        if not selected:
            messagebox.showinfo("情報", "ブラウザでフォルダまたはファイルを選択してください。")
            return
        node_id = selected[0]
        meta = self.browser[side]["meta"].get(node_id)
        if not meta:
            return
        node_type = meta.get("type")
        path = (meta.get("path") or "").strip()
        if node_type == "file":
            self.scope_var.set("exact")
            self.env_vars["EXACT_KEY"].set(path)
            self.env_vars["PREFIX"].set("")
            self._append_log(f"[INFO] 同期対象をファイルに設定: {path}")
        elif node_type in {"folder", "bucket"}:
            if not path:
                self.scope_var.set("all")
                self.env_vars["PREFIX"].set("")
                self.env_vars["EXACT_KEY"].set("")
                self._append_log("[INFO] 同期対象をバケット全体に設定")
            else:
                self.scope_var.set("prefix")
                self.env_vars["PREFIX"].set(path)
                self.env_vars["EXACT_KEY"].set("")
                self._append_log(f"[INFO] 同期対象をフォルダに設定: {path}")
        else:
            messagebox.showinfo("情報", "この項目は同期対象に設定できません。")
            return
        self._refresh_scope_state()

    def _append_log(self, text: str) -> None:
        text = text.rstrip("\n")
        if not text:
            return
        tag = None
        if any(p in text for p in ERROR_PATTERNS):
            tag = "error"
        elif "WARNING" in text or "warn" in text.lower():
            tag = "warn"
        elif "All checks passed." in text or "Summary:" in text:
            tag = "ok"
        self.log_text.insert(tk.END, text + "\n", tag)
        self.log_text.see(tk.END)

    def _drain_log_queue(self) -> None:
        while True:
            try:
                line = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self._append_log(line)
        self.root.after(150, self._drain_log_queue)

    def clear_log(self) -> None:
        self.log_text.delete("1.0", tk.END)

    def pick_env_file(self) -> None:
        picked = filedialog.askopenfilename(
            title=".env を選択",
            filetypes=[("Env files", ".env*"), ("All files", "*.*")],
        )
        if picked:
            self.env_file_var.set(picked)

    def load_env(self) -> None:
        path = Path(self.env_file_var.get().strip())
        data = parse_env_file(path)
        for key in ENV_KEY_ORDER:
            self.env_vars[key].set(data.get(key, DEFAULT_ENV.get(key, "")))
        self.log_level_var.set((data.get("LOG_LEVEL") or "INFO").upper())
        self.full_response_var.set(env_to_bool(data.get("FULL_RESPONSE", "false")))
        self.debug_botocore_var.set(env_to_bool(data.get("DEBUG_BOTOCORE", "false")))
        self.traceback_var.set(env_to_bool(data.get("TRACEBACK_ON_ERROR", "false")))
        self._append_log(f"[INFO] .env 読込: {path}")
        self._refresh_scope_from_env()
        self._update_summary()

    def save_env(self) -> None:
        path = Path(self.env_file_var.get().strip())
        if not path.name:
            messagebox.showerror("エラー", ".env パスが空です。")
            return
        data = self._collect_env_from_ui()
        write_env_file(path, data)
        self._append_log(f"[INFO] .env 保存: {path}")
        self._update_summary()

    def _refresh_scope_from_env(self) -> None:
        exact_key = self.env_vars["EXACT_KEY"].get().strip()
        prefix = self.env_vars["PREFIX"].get().strip()
        if exact_key:
            self.scope_var.set("exact")
        elif prefix:
            self.scope_var.set("prefix")
        else:
            self.scope_var.set("all")
        self._refresh_scope_state()

    def _refresh_scope_state(self) -> None:
        mode = self.scope_var.get()
        if mode == "all":
            self.prefix_entry.configure(state="disabled")
            self.exact_entry.configure(state="disabled")
            self.env_vars["PREFIX"].set("")
            self.env_vars["EXACT_KEY"].set("")
        elif mode == "prefix":
            self.prefix_entry.configure(state="normal")
            self.exact_entry.configure(state="disabled")
            self.env_vars["EXACT_KEY"].set("")
        else:
            self.prefix_entry.configure(state="disabled")
            self.exact_entry.configure(state="normal")
            self.env_vars["PREFIX"].set("")
        self._update_summary()

    def _apply_safety_mode(self) -> None:
        safety = self.safety_mode_var.get()
        if safety:
            self.dry_run_var.set(True)
            self.dry_run_cb.state(["disabled"])
        else:
            self.dry_run_cb.state(["!disabled"])
        self._update_summary()

    def _collect_env_from_ui(self) -> OrderedDict[str, str]:
        data: OrderedDict[str, str] = OrderedDict()
        for key in ENV_KEY_ORDER:
            if key == "LOG_LEVEL":
                data[key] = self.log_level_var.get().strip().upper() or "INFO"
                continue
            data[key] = self.env_vars[key].get().strip()

        data["LOG_LEVEL"] = self.log_level_var.get().strip().upper() or "INFO"
        data["FULL_RESPONSE"] = "true" if self.full_response_var.get() else "false"
        data["DEBUG_BOTOCORE"] = "true" if self.debug_botocore_var.get() else "false"
        data["TRACEBACK_ON_ERROR"] = "true" if self.traceback_var.get() else "false"
        data["PREFIX"] = self.env_vars["PREFIX"].get().strip() if self.scope_var.get() == "prefix" else ""
        data["EXACT_KEY"] = self.env_vars["EXACT_KEY"].get().strip() if self.scope_var.get() == "exact" else ""
        return data

    def _update_summary(self) -> None:
        src_ak = mask_ak(self.env_vars["SRC_AWS_ACCESS_KEY_ID"].get())
        dst_ak = mask_ak(self.env_vars["DST_AWS_ACCESS_KEY_ID"].get())
        scope = self.scope_var.get()
        if scope == "all":
            scope_text = "バケット全体"
        elif scope == "prefix":
            scope_text = f"フォルダ={self.env_vars['PREFIX'].get().strip() or '(空)'}"
        else:
            scope_text = f"ファイル={self.env_vars['EXACT_KEY'].get().strip() or '(空)'}"

        summary = (
            f"ソース: endpoint={self.env_vars['SRC_S3_ENDPOINT'].get().strip() or '<aws-default>'}, "
            f"style={self.env_vars['SRC_S3_ADDRESSING_STYLE'].get().strip() or 'auto'}, "
            f"AK={src_ak}\n"
            f"ターゲット: endpoint={self.env_vars['DST_S3_ENDPOINT'].get().strip() or '<aws-default>'}, "
            f"style={self.env_vars['DST_S3_ADDRESSING_STYLE'].get().strip() or 'auto'}, "
            f"AK={dst_ak}\n"
            f"同期範囲: {scope_text}\n"
            f"事前確認のみ={self.dry_run_var.get()}, 余分削除={self.delete_var.get()}, "
            f"安全運転モード={self.safety_mode_var.get()}"
        )
        self.summary_var.set(summary)

    def _spawn_process(self, cmd: List[str]) -> None:
        if self.process is not None and self.process.poll() is None:
            messagebox.showwarning("実行中", "すでに処理が実行中です。")
            return

        self.process = subprocess.Popen(
            cmd,
            cwd=str(Path.cwd()),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        def reader() -> None:
            assert self.process is not None
            proc = self.process
            if proc.stdout is not None:
                for line in proc.stdout:
                    self.log_queue.put(line.rstrip("\n"))
            code = proc.wait()
            self.log_queue.put(f"[INFO] プロセス終了: exit_code={code}")
            self.process = None

        threading.Thread(target=reader, daemon=True).start()

    def _build_common_args(self) -> List[str]:
        data = self._collect_env_from_ui()
        args = [
            "--env-file",
            str(Path(self.env_file_var.get().strip())),
            "--log-level",
            data["LOG_LEVEL"],
        ]

        if data["LOG_FILE"]:
            args += ["--log-file", data["LOG_FILE"]]

        if self.scope_var.get() == "prefix" and data["PREFIX"]:
            args += ["--prefix", data["PREFIX"]]
        elif self.scope_var.get() == "exact" and data["EXACT_KEY"]:
            args += ["--exact-key", data["EXACT_KEY"]]

        if self.dry_run_var.get():
            args.append("--dry-run")
        if self.delete_var.get():
            args.append("--delete")
        if self.copy_tags_var.get():
            args.append("--copy-tags")
        if self.force_var.get():
            args.append("--force")
        if self.full_response_var.get():
            args.append("--full-response")
        else:
            args.append("--no-full-response")
        if self.debug_botocore_var.get():
            args.append("--debug-botocore")
        if self.traceback_var.get():
            args.append("--traceback-on-error")

        args += ["--workers", data["WORKERS"]]
        args += ["--multipart-threshold-mb", data["MULTIPART_THRESHOLD_MB"]]
        args += ["--multipart-chunk-mb", data["MULTIPART_CHUNK_MB"]]
        args += ["--progress-every", data["PROGRESS_EVERY"]]
        return args

    def run_test(self, which: str) -> None:
        self.save_env()
        cmd = [
            sys.executable,
            str(Path.cwd() / "test_aws_key.py"),
            "--which",
            which,
            "--list-objects",
            "3",
            "--env-file",
            str(Path(self.env_file_var.get().strip())),
            "--log-level",
            self.log_level_var.get().strip().upper() or "INFO",
        ]
        if self.full_response_var.get():
            cmd.append("--full-response")
        else:
            cmd.append("--no-full-response")
        if self.debug_botocore_var.get():
            cmd.append("--debug-botocore")
        if self.traceback_var.get():
            cmd.append("--traceback-on-error")

        self._append_log(f"[INFO] 実行: {' '.join(cmd)}")
        self._spawn_process(cmd)

    def run_sync(self) -> None:
        self.save_env()
        self._update_summary()

        if self.scope_var.get() == "exact" and not self.env_vars["EXACT_KEY"].get().strip():
            messagebox.showerror("入力エラー", "ファイル単位同期ではファイルパスが必要です。")
            return

        if self.scope_var.get() == "prefix" and not self.env_vars["PREFIX"].get().strip():
            if not messagebox.askyesno("確認", "フォルダパスが空です。バケット全体同期になります。続行しますか？"):
                return

        if self.delete_var.get():
            msg = "余分ファイル削除が有効です。ターゲット側の不要オブジェクトが削除されます。実行しますか？"
            if not messagebox.askyesno("危険操作の確認", msg):
                return

        if not self.safety_mode_var.get() and not self.dry_run_var.get():
            if not messagebox.askyesno("最終確認", "本実行（事前確認モードOFF）です。続行しますか？"):
                return

        cmd = [sys.executable, str(Path.cwd() / "sync_s3_cross_account.py")]
        cmd += self._build_common_args()

        self._append_log(f"[INFO] 実行: {' '.join(cmd)}")
        self._spawn_process(cmd)

    def stop_process(self) -> None:
        if self.process is None or self.process.poll() is not None:
            self._append_log("[INFO] 停止対象の実行プロセスはありません。")
            return

        try:
            self.process.terminate()
            self._append_log("[WARNING] 停止要求を送信しました。")
        except Exception as exc:
            self._append_log(f"[ERROR] 停止失敗: {exc}")


def launch_gui() -> int:
    root = tk.Tk()
    style = ttk.Style()
    try:
        style.theme_use("clam")
    except Exception:
        pass
    app = SyncGuiApp(root)
    app._append_log("[INFO] GUI 起動完了。")
    root.mainloop()
    return 0


if __name__ == "__main__":
    if "--gui" not in sys.argv:
        print("このGUIは --gui パラメータ付きで起動してください。例: python s3_sync_tk_gui.py --gui")
        sys.exit(2)
    sys.exit(launch_gui())
