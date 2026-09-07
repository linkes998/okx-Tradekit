"""Patch rh_server_live.py to add DeskRunner integration."""
with open("rh_server_live.py", "r", encoding="utf-8") as f:
    c = f.read()

# 1. Add import at top
old_import_end = "from pathlib import Path\nfrom urllib.parse import parse_qs, urlparse"
new_import_end = old_import_end + "\n\nfrom rh_desk_runner import DeskRunner"
c = c.replace(old_import_end, new_import_end)

# 2. Update _sse_handler
old_sse_marker = "    def _sse_handler(self, u):\n        \"\"\"Server-Sent Events stream: desk state + pending swaps.\"\"\""

new_sse = '''    def _sse_handler(self, u):
        """Server-Sent Events stream.

        Two modes:
          - DeskRunner push mode: reads full state (equity/positions/swaps) from _live_queue
          - Manual poll mode: polls swaps+status every 2s (fallback when no DeskRunner)
        """
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        if not _server:
            self.wfile.write(b'data: {"error":"no server running"}\\n\\n')
            return

        saved = os.environ.get("RH_USER_WALLET", "")
        if saved and not _server.user_wallet:
            _server.user_wallet = saved
            _server.trader.user_wallet = saved

        try:
            if _live_queue is not None:
                print("[SSE] DeskRunner push mode — reading from _live_queue")
                while True:
                    try:
                        state = _live_queue.get(timeout=10)
                        payload = json.dumps(state, default=str)
                        self.wfile.write(f"data: {payload}\\n\\n".encode())
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b"event: ping\\ndata: \\n\\n")
                        self.wfile.flush()
            else:
                print("[SSE] Manual poll mode — no DeskRunner")
                initial = _server.trader.status()
                init_payload = json.dumps({
                    "swaps": _server.swaps_snapshot(),
                    "status": initial,
                    "sol_usd": initial.get("sol_usd", 0),
                }, default=str)
                self.wfile.write(f"data: {init_payload}\\n\\n".encode())
                self.wfile.flush()
                while True:
                    try:
                        if not _server._sol_price_fetched:
                            _server.refresh_sol_price()
                    except Exception:
                        pass
                    swaps_data = _server.swaps_snapshot()
                    status_data = _server.trader.status()
                    payload = json.dumps({
                        "swaps": swaps_data, "status": status_data,
                        "sol_usd": status_data.get("sol_usd", 0),
                    }, default=str)
                    self.wfile.write(f"data: {payload}\\n\\n".encode())
                    self.wfile.flush()
                    time.sleep(2)
        except (BrokenPipeError, ConnectionResetError):
            pass'''

# Find and replace old _sse_handler - match from marker to end of method
# Just replace the signature line and docstring with new
c = c.replace(
    '    def _sse_handler(self, u):\n        """Server-Sent Events stream: desk state + pending swaps."""',
    new_sse
)

# Remove the old SSE body - find `prev_swap_keys = set` and everything after until next `def `
old_body_start = '\n        if not _server:\n            self.wfile.write(b"data: {\\"error\\":\\"no server running\\"}\\n\\n")\n            return\n\n        try:'
old_body_end = '\n        except (BrokenPipeError, ConnectionResetError):\n            pass\n\n\nSERVER_DEFAULTS'

# Actually let's just find where the old body starts/ends and cut it out
# Find "def main(" as anchor
main_marker = "def main(host"
main_idx = c.index(main_marker)

# Find the _sse_handler body after the new_sse we inserted - it will have duplicate code
# Let me re-read the file to figure out what happened

# Instead, let's take a different approach: write the whole patched file
with open("rh_server_live.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Patch step 1: import + _sse_handler updated")
