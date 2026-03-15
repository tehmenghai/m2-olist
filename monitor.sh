#!/usr/bin/env bash
# Agent Teams Visual Monitor
# Launches a tmux session with a 2x2 grid for monitoring Claude Code agents.
# Usage: bash monitor.sh
#        or via VS Code: Terminal > Run Task > ▶ Agent Monitor

SESSION="claude-agents"
WORKDIR="$(cd "$(dirname "$0")" && pwd)"

# Kill existing session if present
tmux kill-session -t "$SESSION" 2>/dev/null

# Create new session (detached), sized for a typical VS Code terminal
tmux new-session -d -s "$SESSION" -x 220 -y 50 -n "agents"

# Build 2x2 grid:
#   pane 0 (top-left)  → MAIN
#   pane 1 (top-right) → AGENT-2
#   pane 2 (bot-left)  → AGENT-3
#   pane 3 (bot-right) → AGENT-4
tmux split-window -h -t "$SESSION:0"        # creates pane 1 (right of pane 0)
tmux split-window -v -t "$SESSION:0.0"      # creates pane 2 (below pane 0)
tmux split-window -v -t "$SESSION:0.1"      # creates pane 3 (below pane 1)

# Label each pane (visible in tmux status / pane border)
tmux select-pane -t "$SESSION:0.0" -T "MAIN"
tmux select-pane -t "$SESSION:0.1" -T "AGENT-2"
tmux select-pane -t "$SESSION:0.2" -T "AGENT-3"
tmux select-pane -t "$SESSION:0.3" -T "AGENT-4"

# Enable pane border titles
tmux set-option -t "$SESSION" pane-border-status top
tmux set-option -t "$SESSION" pane-border-format " #{pane_title} "

# Start Claude Code in main pane
tmux send-keys -t "$SESSION:0.0" "cd '$WORKDIR' && claude" Enter

# Focus main pane and attach
tmux select-pane -t "$SESSION:0.0"
tmux attach-session -t "$SESSION"
