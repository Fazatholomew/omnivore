#!/bin/bash

# Navigate to your project directory
cd /home/ubuntu/omnivore

# Activate the virtual environment
source .venv/bin/activate

# Run your Python script
make start

# Optionally deactivate the virtual environment
deactivate
