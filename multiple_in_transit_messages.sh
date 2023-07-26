#!/bin/bash
cd "$(dirname "$0")"
echo "Start time: $(date)" >> log.txt
./venv/bin/python multiple_in_transit_messages.py >> log.txt &
wait
