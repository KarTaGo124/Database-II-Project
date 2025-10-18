#!/bin/bash
# Installation script for test dependencies

echo "================================================"
echo "Installing Dependencies for Index Performance Tests"
echo "================================================"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

# Activate virtual environment and install dependencies
echo "Installing required packages..."
source venv/bin/activate

pip install --upgrade pip
pip install pandas matplotlib seaborn numpy

echo ""
echo "================================================"
echo "Installation complete!"
echo "================================================"
echo ""
echo "To run the tests, use:"
echo "  source venv/bin/activate"
echo "  python tests.py"
echo ""
echo "Or run the convenience script:"
echo "  ./run_tests.sh"
echo ""
