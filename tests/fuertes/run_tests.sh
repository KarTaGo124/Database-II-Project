#!/bin/bash
# Convenience script to run tests with virtual environment

echo "================================================"
echo "Running Index Performance Tests"
echo "================================================"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Virtual environment not found!"
    echo "Please run ./install_dependencies.sh first"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Run tests
python tests.py

# Deactivate when done
deactivate

echo ""
echo "Tests completed! Check test_results/ for output."
