#!/bin/bash

cd "$(dirname "$0")"
# Create .venv if it doesn't exist
[ -d ".venv" ] || python -m venv .venv

# Activate env
source .venv/Scripts/activate

# Upgrade and install
python -m pip install --upgrade pip
pip install -r docker/requirements.txt

echo "Environment ready. Run 'source .venv/Scripts/activate' to use it."
