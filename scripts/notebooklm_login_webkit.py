#!/usr/bin/env python3
"""Log into NotebookLM using Playwright WebKit (native Safari engine on macOS).

Chromium crashes with BUS_ADRALN on Apple Silicon. WebKit is native and stable.
Saves storage_state.json to ~/.notebooklm/profiles/default/ for notebooklm-py.
"""

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

NOTEBOOKLM_URL = "https://notebooklm.google.com/"
GOOGLE_ACCOUNTS_URL = "https://accounts.google.com/"

STORAGE_DIR = Path.home() / ".notebooklm" / "profiles" / "default"
STORAGE_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
STORAGE_PATH = STORAGE_DIR / "storage_state.json"


def main():
    print("Launching Safari-based browser for Google login...")
    print(f"Profile dir: {STORAGE_DIR}\n")

    with sync_playwright() as p:
        context = p.webkit.launch_persistent_context(
            user_data_dir=str(STORAGE_DIR / "browser_profile"),
            headless=False,
        )

        page = context.pages[0] if context.pages else context.new_page()
        page.goto(NOTEBOOKLM_URL)

        print("=" * 60)
        print("1. Complete Google login in the browser window")
        print("2. Wait until you see the NotebookLM homepage")
        print("3. Come back here and press ENTER")
        print("=" * 60)

        input("\n[Press ENTER when logged in] ")

        # Refresh cookies by visiting Google accounts + NotebookLM
        page.goto(GOOGLE_ACCOUNTS_URL, wait_until="load")
        page.goto(NOTEBOOKLM_URL, wait_until="load")

        current_url = page.url
        if "notebooklm.google.com" not in current_url:
            print(f"\nWarning: Not on NotebookLM (current: {current_url})")
            if input("Save anyway? [y/N] ").lower() != "y":
                context.close()
                return

        context.storage_state(path=str(STORAGE_PATH))
        STORAGE_PATH.chmod(0o600)
        context.close()

    print(f"\nAuth saved to: {STORAGE_PATH}")
    print(f"Profile: default")

    # Verify
    from notebooklm.cli.session import _output_auth_check as output_check
    from notebooklm.auth import extract_cookies_from_storage, fetch_tokens
    from notebooklm.paths import get_storage_path

    storage = get_storage_path()
    if storage.exists():
        state = json.loads(storage.read_text())
        cookies = extract_cookies_from_storage(state)
        checks = {
            "storage_exists": True,
            "json_valid": True,
            "cookies_present": bool(cookies),
            "sid_cookie": "SID" in cookies,
            "token_fetch": None,
        }
        try:
            import asyncio
            csrf, sid = asyncio.get_event_loop().run_until_complete(fetch_tokens(cookies))
            checks["token_fetch"] = bool(csrf and sid)
        except Exception:
            checks["token_fetch"] = False
        output_check(checks, {"storage_path": str(storage), "auth_source": "file"}, json_output=False)


if __name__ == "__main__":
    main()
