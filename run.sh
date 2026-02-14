#!/bin/bash

echo "========================================"
echo "Starting Dashboard"
echo "Time: $(date)"
echo "========================================"

# Set working directory to script location
cd "$(dirname "$0")" || exit 1

# Set Python encoding
export PYTHONIOENCODING=utf-8
# Alternative for Python 3.7+ to force UTF-8 mode
export PYTHONUTF8=1

# Activate conda environment
echo "Activating conda environment ccxt_dashboard..."
# Adjust the path to your conda installation if needed
source ~/miniconda3/etc/profile.d/conda.sh 2>/dev/null || source ~/anaconda3/etc/profile.d/conda.sh 2>/dev/null
conda activate ccxt_dashboard

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to activate Conda environment"
    exit 1
fi

# Check if run.py exists
if [ ! -f "run.py" ]; then
    echo "ERROR: File run.py not found in current directory"
    echo "Current directory: $(pwd)"
    exit 1
fi

echo "Starting server..."
echo "========================================"

# Run with UTF-8 mode and append output to run.log
python -X utf8 run.py >> run.log 2>&1