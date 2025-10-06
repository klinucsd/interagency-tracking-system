#!/bin/bash

# Exit on any error
set -e

echo "=== Installing GDAL with FileGDB API Support ==="

echo "Checking for existing GDAL installation..."
if command -v gdalinfo >/dev/null 2>&1; then
    GDAL_VERSION=$(gdalinfo --version 2>&1 | head -n1)
    echo "GDAL is already installed: $GDAL_VERSION"
    echo "Skipping installation to avoid changing existing settings."
    echo ""
    echo "Current FileGDB driver status:"
    ogrinfo --formats | grep -i "filegdb\|openfilegdb" || echo "FileGDB driver not found in current installation."
    exit 0
else
    echo "GDAL not found. Proceeding with installation..."
fi

# Install dependencies
echo "Step 1: Installing dependencies..."
sudo apt-get update
sudo apt-get install -y cmake build-essential libsqlite3-dev \
  libproj-dev libtiff-dev libcurl4-openssl-dev libxml2-dev \
  zlib1g-dev libssl-dev

# Download and install FileGDB API
echo "Step 2: Downloading FileGDB API..."
cd /tmp
wget https://raw.githubusercontent.com/Esri/file-geodatabase-api/refs/heads/master/FileGDB_API_1.5.2/FileGDB_API-RHEL7-64gcc83.tar.gz
tar xfvz FileGDB_API-RHEL7-64gcc83.tar.gz
sudo mv FileGDB_API-RHEL7-64gcc83 /opt/

# Remove incompatible libstdc++
echo "Step 3: Removing incompatible libstdc++ from FileGDB..."
cd /opt/FileGDB_API-RHEL7-64gcc83/lib/
sudo mkdir -p backup
sudo mv libstdc++* backup/ 2>/dev/null || true

# Download and extract GDAL
echo "Step 4: Downloading GDAL..."
cd /tmp
wget https://github.com/OSGeo/gdal/releases/download/v3.8.4/gdal-3.8.4.tar.gz
tar xfvz gdal-3.8.4.tar.gz
cd gdal-3.8.4
mkdir -p build
cd build

# Configure and build GDAL
echo "Step 5: Configuring and building GDAL..."
export FILEGDB_ROOT=/opt/FileGDB_API-RHEL7-64gcc83
export LDFLAGS="-L${FILEGDB_ROOT}/lib -Wl,-rpath,${FILEGDB_ROOT}/lib"
export CPPFLAGS="-I${FILEGDB_ROOT}/include"

cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr/local \
  -DFileGDB_ROOT=$FILEGDB_ROOT \
  -DGDAL_USE_FILEGDB=ON \
  -DCMAKE_INSTALL_RPATH="${FILEGDB_ROOT}/lib" \
  -DCMAKE_BUILD_RPATH="${FILEGDB_ROOT}/lib"

make -j$(nproc)

# Install GDAL
echo "Step 6: Installing GDAL..."
sudo make install

# Configure library path
echo "Step 7: Configuring system library path..."
echo "/opt/FileGDB_API-RHEL7-64gcc83/lib" | sudo tee /etc/ld.so.conf.d/filegdb.conf
sudo ldconfig

# Verify installation
echo "Step 8: Verifying installation..."
gdalinfo --version
echo ""
echo "Available FileGDB drivers:"
ogrinfo --formats | grep -i "filegdb\|openfilegdb"

echo ""
echo "=== Installation completed successfully! ==="
