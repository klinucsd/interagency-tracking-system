FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Set working directory
WORKDIR /app

# Install system dependencies including Python
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libsqlite3-dev \
    libproj-dev \
    libtiff-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    zlib1g-dev \
    libssl-dev \
    wget \
    git \
    unzip \
    vim \
    python3 \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Download and install FileGDB API
RUN cd /tmp && \
    wget https://raw.githubusercontent.com/Esri/file-geodatabase-api/refs/heads/master/FileGDB_API_1.5.2/FileGDB_API-RHEL7-64gcc83.tar.gz && \
    tar xfvz FileGDB_API-RHEL7-64gcc83.tar.gz && \
    mv FileGDB_API-RHEL7-64gcc83 /opt/ && \
    rm FileGDB_API-RHEL7-64gcc83.tar.gz

# Remove incompatible libstdc++ from FileGDB
RUN cd /opt/FileGDB_API-RHEL7-64gcc83/lib/ && \
    mkdir -p backup && \
    mv libstdc++* backup/ 2>/dev/null || true

# Download and build GDAL with FileGDB support
ENV FILEGDB_ROOT=/opt/FileGDB_API-RHEL7-64gcc83
ENV LDFLAGS="-L${FILEGDB_ROOT}/lib -Wl,-rpath,${FILEGDB_ROOT}/lib"
ENV CPPFLAGS="-I${FILEGDB_ROOT}/include"

RUN cd /tmp && \
    wget https://github.com/OSGeo/gdal/releases/download/v3.8.4/gdal-3.8.4.tar.gz && \
    tar xfvz gdal-3.8.4.tar.gz && \
    cd gdal-3.8.4 && \
    mkdir -p build && \
    cd build && \
    cmake .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DFileGDB_ROOT=$FILEGDB_ROOT \
      -DGDAL_USE_FILEGDB=ON \
      -DCMAKE_INSTALL_RPATH="${FILEGDB_ROOT}/lib" \
      -DCMAKE_BUILD_RPATH="${FILEGDB_ROOT}/lib" && \
    make -j$(nproc) && \
    make install && \
    cd /tmp && \
    rm -rf gdal-3.8.4*

# Configure library path
RUN echo "/opt/FileGDB_API-RHEL7-64gcc83/lib" > /etc/ld.so.conf.d/filegdb.conf && \
    ldconfig

# Clone the repository
RUN git clone https://github.com/klinucsd/interagency-tracking-system.git /app/interagency-tracking-system

# Set working directory to the cloned repo
WORKDIR /app/interagency-tracking-system

# Install Python requirements
RUN pip3 install --no-cache-dir -r requirements.txt

# Verify GDAL installation
RUN gdalinfo --version && \
    ogrinfo --formats | grep -i "filegdb\|openfilegdb"

# Set environment variables for runtime
ENV LD_LIBRARY_PATH=/opt/FileGDB_API-RHEL7-64gcc83/lib:$LD_LIBRARY_PATH
ENV PATH=/usr/local/bin:$PATH

# Default command (you can override this)
CMD ["/bin/bash"]
