#!/bin/bash

set -e

# Define paths
BUILD_DIR="lambda_build_temp"
ZIP_FILE="lambda_build.zip"

# Cleanup any previous build
rm -rf "$BUILD_DIR" "$ZIP_FILE"

# Create build dir
mkdir -p "$BUILD_DIR"

# Install Python dependencies into build dir
pip install -r requirements.txt -t "$BUILD_DIR"

# Copy contents of lambda directory into build dir
cp -r lambda/* "$BUILD_DIR/"

# Create zip from build dir contents
(cd "$BUILD_DIR" && zip -r "../$ZIP_FILE" .)

# Clean up build directory
rm -rf "$BUILD_DIR"

echo "Created $ZIP_FILE successfully."
