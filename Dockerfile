FROM ubuntu:24.04

# Install system dependencies, including gdal-bin, libgdal-dev, python3-pip, and Docker prerequisites
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    git wget unzip emacs-nox python3-venv python3-dev \
    libproj-dev libgeos-dev libjson-c-dev libcurl4-gnutls-dev libsqlite3-dev \
    libexpat-dev libxerces-c-dev libtiff-dev libpng-dev libjpeg-dev \
    libwebp-dev libopenjp2-7-dev libcairo2-dev libpoppler-dev libpcre3-dev \
    libnetcdf-dev libhdf5-dev libfyba-dev libkml-dev libzstd-dev liblerc-dev \
    build-essential curl gnupg lsb-release \
    gdal-bin libgdal-dev \
    ca-certificates python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Docker
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin && \
    rm -rf /var/lib/apt/lists/*

# Modify /etc/init.d/docker to bypass ulimit error
RUN sed -i 's/ulimit -Hn/#ulimit -Hn/' /etc/init.d/docker

# Clone the repo
RUN git clone https://github.com/klinucsd/interagency-tracking-system /app

WORKDIR /app

# Install Python libraries system-wide (GDAL Python bindings using system GDAL)
RUN PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig pip3 install --no-cache-dir --break-system-packages gdal==$(gdal-config --version) --no-binary gdal && \
    pip3 install --no-cache-dir --break-system-packages -r requirements.txt 

# Default command: drop into a shell for interactive work
CMD /bin/bash -c "service docker start && /bin/bash"
