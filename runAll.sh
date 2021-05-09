#!/bin/bash
cd /root/stocksnake/
source .venv/bin/activate
python src/startGrabbing.py
python src/startAnalysis.py

