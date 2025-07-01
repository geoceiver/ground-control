# Ground Control

A Rust library and API for GNSS data processing and satellite orbit calculations using Restate workflows. Provides real-time access to satellite positions and processes SP3 precision orbit files.

## Overview

Ground Control is a GNSS data processing library that provides satellite orbit calculations, SP3 file processing, and real-time satellite position queries. It uses workflow orchestration to ensure reliable data processing with automatic retries and state management.

## Architecture

The application is built using:

- **Restate SDK**: Workflow orchestration and durable execution
- **Tokio**: Async runtime for high-performance I/O
- **Axum**: Web framework for REST API endpoints
- **ANISE**: Astrodynamics library for precise orbit calculations
- **Arrow**: Columnar data strucutre for efficient in-memory SP3 handling

### Core Components

- **`main.rs`**: Application entry point with HTTP server and Restate workflows
- **`data/sp3.rs`**: SP3 file parsing and processing workflows
- **`product/sv.rs`**: Satellite orbit calculations and data source management
- **`algo/util.rs`**: Algorithmic utilities for GNSS processing
- **`gpst.rs`**: GPS time system conversions and utilities

## Features

- **Real-time Orbit Queries**: Get satellite positions for any epoch
- **SP3 File Processing**: Parse and process precision orbit files
- **Multi-source Support**: Handle different GNSS data sources (IGS, CODE, etc.)
- **REST API**: HTTP endpoints for satellite position queries
- **Batch Processing**: Process multiple satellites and epochs efficiently
- **Time System Support**: GPS time and UTC conversions
- **Workflow Orchestration**: Reliable processing with automatic retries

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```bash
# CDDIS Authentication (for data access)
EARTHDATA_TOKEN=your_earthdata_token

# Cloud Storage (for SP3 file access)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

## Usage

### Running the Service

```bash
# Install dependencies
cargo build --release

# Run the service
cargo run
```

The service starts two servers:

- HTTP API server on `0.0.0.0:3010` for REST endpoints
- Restate workflow server on `0.0.0.0:9080` for workflow processing

### API Endpoints

```bash
# Get available data sources
GET /orbit/sources

# Get orbit for specific satellite at epoch
GET /orbit/{source}/{sv}/{epoch}

# Get orbits for all satellites at epoch
GET /orbits/{source}/{epoch}

# Process SP3 file
POST /orbit/source
```

### Data Sources

Supported GNSS data sources:

- `igs`: International GNSS Service final products
- `igr`: IGS rapid products
- `cod`: Center for Orbit Determination products
- Custom sources can be added via configuration

### Testing

```bash
# Run unit tests
cargo test

# Run SP3 processing tests
cargo test --test sp3_tests
```

## File Structure

```
src/
├── main.rs          # Application entry point and API server
├── lib.rs           # Library exports
├── gpst.rs          # GPS time system utilities
├── algo/
│   ├── mod.rs       # Algorithm module exports
│   └── util.rs      # Algorithmic utilities
├── data/
│   ├── mod.rs       # Data module exports
│   ├── sp3.rs       # SP3 file processing workflows
│   └── rtcm.rs      # RTCM data handling
└── product/
    ├── mod.rs       # Product module exports
    ├── sv.rs        # Satellite orbit calculations
    └── gs.rs        # Ground station products

tests/
└── sp3_tests.rs     # SP3 file processing tests
```
