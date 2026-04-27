#!/usr/bin/env python3
"""Prepare NyoSig v7.5c runtime files from v7.5a + patch.
This keeps the full v7.5a core in the repository and generates v7.5c before startup.
"""
from __future__ import annotations
import os
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent
PATCH_DIR = ROOT / "patches"


def _read_text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8-sig")


def _write_text(path: pathlib.Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _apply_unified_patch(original_text: str, patch_text: str) -> str:
    """Small unified diff applier for the patches generated in this release."""
    source = original_text.splitlines(keepends=True)
    output: list[str] = []
    src_index = 0
    lines = patch_text.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.startswith("@@ "):
            i += 1
            continue
        match = re.match(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
        if not match:
            raise RuntimeError("Invalid patch hunk header: " + line.strip())
        old_start = int(match.group(1)) - 1
        if old_start < src_index:
            raise RuntimeError("Patch hunks are out of order")
        output.extend(source[src_index:old_start])
        src_index = old_start
        i += 1
        while i < len(lines) and not lines[i].startswith("@@ "):
            h = lines[i]
            if h.startswith(" "):
                expected = h[1:]
                actual = source[src_index] if src_index < len(source) else None
                if actual != expected:
                    raise RuntimeError(
                        "Patch context mismatch at source line "
                        + str(src_index + 1)
                        + ": expected "
                        + repr(expected[:120])
                        + " got "
                        + repr((actual or "")[:120])
                    )
                output.append(actual)
                src_index += 1
            elif h.startswith("-"):
                expected = h[1:]
                actual = source[src_index] if src_index < len(source) else None
                if actual != expected:
                    raise RuntimeError(
                        "Patch remove mismatch at source line "
                        + str(src_index + 1)
                        + ": expected "
                        + repr(expected[:120])
                        + " got "
                        + repr((actual or "")[:120])
                    )
                src_index += 1
            elif h.startswith("+"):
                output.append(h[1:])
            elif h.startswith("\\"):
                pass
            else:
                raise RuntimeError("Unsupported patch line: " + h[:120])
            i += 1
    output.extend(source[src_index:])
    return "".join(output)


def _generate_from_patch(source_name: str, target_name: str, patch_name: str) -> None:
    src = ROOT / source_name
    dst = ROOT / target_name
    patch = PATCH_DIR / patch_name
    if not src.is_file():
        print("WARN: missing source file", src)
        return
    if not patch.is_file():
        print("WARN: missing patch file", patch)
        return
    try:
        text = _apply_unified_patch(_read_text(src), _read_text(patch))
        _write_text(dst, text)
        print("OK: generated", target_name)
    except Exception as exc:
        print("WARN: could not generate", target_name, "from patch:", exc)
        if dst.is_file():
            print("OK: existing", target_name, "left unchanged")


def _patch_api() -> None:
    path = ROOT / "nyosig_api.py"
    if not path.is_file():
        print("WARN: missing nyosig_api.py")
        return
    text = _read_text(path)
    original = text
    text = text.replace("Wraps core v7.5a as REST endpoints", "Wraps core v7.5c as REST endpoints")
    text = text.replace(
        'for name in ["nyosig_analysator_core_v7.5a.py", "nyosig_analysator_core_v8.0a.py"]:',
        'for name in ["nyosig_analysator_core_v7.5c.py", "nyosig_analysator_core_v7.5a.py", "nyosig_analysator_core_v8.0a.py"]:',
    )
    text = text.replace('APP_VERSION = "v8.1-web"', 'APP_VERSION = "v7.5c-web"')
    text = text.replace('scopes = [lr["scope_key"] for lr in LAYER_REGISTRY]', 'scopes = [lr["scope_key"] for lr in _core.LAYER_REGISTRY]')
    if text != original:
        _write_text(path, text)
        print("OK: patched nyosig_api.py for v7.5c")
    else:
        print("OK: nyosig_api.py already v7.5c-compatible")


def main() -> int:
    _generate_from_patch(
        "nyosig_analysator_core_v7.5a.py",
        "nyosig_analysator_core_v7.5c.py",
        "core_v7_5a_to_v7_5c.patch",
    )
    _generate_from_patch(
        "nyosig_analysator_gui_v7.5a.py",
        "nyosig_analysator_gui_v7.5c.py",
        "gui_v7_5a_to_v7_5c.patch",
    )
    _patch_api()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
