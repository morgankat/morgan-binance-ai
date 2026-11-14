#!/data/data/com.termux/files/usr/bin/bash
# Megan relay keep-alive: run this ONE time. It keeps the relay running forever, restarts it if it stops,
# and starts it again by itself when the phone reboots or comes back online.
BASE="https://raw.githubusercontent.com/morgankat/morgan-binance-ai/main"
H="$HOME"
mkdir -p "$H/.termux/boot"

# the runner: updates the relay file when online, then restarts it every time it stops
cat > "$H/megan_run.sh" <<'RUN'
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock 2>/dev/null
cd "$HOME"
BASE="https://raw.githubusercontent.com/morgankat/morgan-binance-ai/main"
# only one runner at a time
LOCK="$HOME/.megan_run.pid"
if [ -f "$LOCK" ] && kill -0 "$(cat "$LOCK")" 2>/dev/null; then exit 0; fi
echo $$ > "$LOCK"
while true; do
  # if a relay is already running, never fight it: just wait
  if pgrep -f "python.*binance_relay.py" > /dev/null; then sleep 20; continue; fi
  curl -fsL -m 40 "$BASE/binance_relay.py" -o "$HOME/binance_relay.new" && [ -s "$HOME/binance_relay.new" ] && python -m py_compile "$HOME/binance_relay.new" 2>/dev/null && mv "$HOME/binance_relay.new" "$HOME/binance_relay.py"
  T0=$(date +%s)
  python "$HOME/binance_relay.py" >> "$HOME/relay.log" 2>&1
  RC=$?
  echo "$(date) relay stopped (exit $RC), restarting soon" >> "$HOME/relay.log"
  [ $(( $(date +%s) - T0 )) -lt 20 ] && sleep 25
  [ "$(stat -c %s "$HOME/relay.log" 2>/dev/null || echo 0)" -gt 2000000 ] && tail -n 2000 "$HOME/relay.log" > "$HOME/relay.log.t" && mv "$HOME/relay.log.t" "$HOME/relay.log"
  sleep 5
done
RUN
chmod +x "$H/megan_run.sh"

# start by itself after a phone reboot (needs the free Termux:Boot app installed once)
cat > "$H/.termux/boot/megan.sh" <<'BOOT'
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
nohup "$HOME/megan_run.sh" > /dev/null 2>&1 &
BOOT
chmod +x "$H/.termux/boot/megan.sh"

# a watcher that starts the runner again if Android ever killed it (checked every 30 seconds)
cat > "$H/megan_watch.sh" <<'WATCH'
#!/data/data/com.termux/files/usr/bin/bash
while true; do
  if ! { [ -f "$HOME/.megan_run.pid" ] && kill -0 "$(cat "$HOME/.megan_run.pid")" 2>/dev/null; }; then nohup "$HOME/megan_run.sh" > /dev/null 2>&1 & fi
  sleep 30
done
WATCH
chmod +x "$H/megan_watch.sh"

pkill -f megan_watch.sh 2>/dev/null
[ -f "$H/.megan_run.pid" ] && kill "$(cat "$H/.megan_run.pid")" 2>/dev/null
pkill -f megan_run.sh 2>/dev/null; rm -f "$H/.megan_run.pid"; pkill -f "python.*binance_relay.py" 2>/dev/null
sleep 1
nohup "$H/megan_run.sh" > /dev/null 2>&1 &
nohup "$H/megan_watch.sh" > /dev/null 2>&1 &
echo "Megan relay is running and will restart itself. Log: ~/relay.log"
