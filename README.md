# Ground Control

A high-performance GNSS data processing and archiving system built with Rust, featuring satellite orbit calculations, real-time position queries, and automated data archiving from NASA's CDDIS.

## Overview

Ground Control is a comprehensive GNSS (Global Navigation Satellite System) data processing platform that provides:

- **Real-time satellite position calculations** with precise orbit data
- **Automated GNSS data archiving** from NASA's CDDIS to cloud storage
- **REST API** for satellite orbit queries and data processing
- **Workflow orchestration** using Restate for reliable, durable execution

The system consists of two main components:

- **Ground Control**: Core library and API for satellite orbit calculations and SP3 file processing
- **CDDIS Archiver**: High-performance data archiving service with parallel processing

## Architecture

Built using modern Rust technologies:

- **Restate SDK**: Workflow orchestration and durable execution
- **Tokio**: Async runtime for high-performance I/O
- **Axum**: Web framework for REST API endpoints
- **ANISE**: Astrodynamics library for precise orbit calculations
- **Arrow**: Columnar data structures for efficient SP3 handling
- **Object Store**: Cloud storage abstraction (AWS S3/Cloudflare R2)

## Features

### Ground Control Library

- Real-time satellite position queries for any epoch
- SP3 precision orbit file processing
- Multi-source GNSS data support (IGS, CODE, etc.)
- GPS time system conversions
- Batch processing for multiple satellites
- RESTful API with HTTP endpoints

### CDDIS Archiver

- Automated archiving of GNSS products from NASA CDDIS
- Incremental processing (only new/changed files)
- Configurable parallel downloads (up to 25 concurrent)
- SHA512 hash validation for data integrity
- Error recovery with exponential backoff
- Recurring scheduled jobs

## Quick Start

### Prerequisites

Create a `.env` file with required credentials:

```bash
# CDDIS Authentication (NASA Earthdata account required)
EARTHDATA_TOKEN=your_earthdata_token

# Cloud Storage Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_ENDPOINT=your_r2_endpoint  # For Cloudflare R2
AWS_REGION=auto
BUCKET_NAME=your_bucket_name
```

### Installation & Running

```bash
# Clone the repository
git clone <repository-url>
cd ground-control

# Build the project
cargo build --release

# Run Ground Control API (port 3010 + 9080)
cargo run --bin ground-control

# Run CDDIS Archiver (port 9080)
cargo run --bin cddis-archiver
```

## API Reference

### Ground Control API Endpoints

```bash
# Get available GNSS data sources
GET /orbit/sources

# Get satellite position at specific epoch
GET /orbit/{source}/{satellite}/{epoch}
# Example: GET /orbit/igs/G01/1234567890.0

# Get all satellite positions at epoch
GET /orbits/{source}/{epoch}
# Example: GET /orbits/igs/1234567890.0

# Process SP3 precision orbit file
POST /orbit/source
Content-Type: application/json
```

### Supported Data Sources

- `igs`: International GNSS Service final products
- `igr`: IGS rapid products
- `cod`: Center for Orbit Determination products
- Custom sources via configuration

## Development

### Project Structure

```
ground-control/
├── Cartgo.toml              # Workspace configuration
├── ground-control/          # Core library and API
│   ├── src/
│   │   ├── main.rs         # API server and workflows
│   │   ├── data/sp3.rs     # SP3 file processing
│   │   ├── product/sv.rs   # Satellite orbit calculations
│   │   ├── algo/util.rs    # GNSS algorithms
│   │   └── gpst.rs         # GPS time utilities
│   └── tests/
├── cddis-archiver/         # Data archiving service
│   ├── src/
│   │   ├── main.rs         # Main workflow orchestration
│   │   ├── archiver.rs     # Core archiving logic
│   │   ├── cddis.rs        # CDDIS API client
│   │   ├── queue.rs        # File processing queue
│   │   └── r2.rs           # Cloud storage operations
│   └── tests/
└── ui/                     # Web interface (separate)
```

### Testing

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --bin ground-control
cargo test --bin cddis-archiver

# Run integration tests
cargo test --test full_archive_test
cargo test --test queue_test
```

### Configuration

#### Archive Request Parameters

```rust
CDDISArchiveRequest {
    request_id: "unique_id",
    parallelism: Some(25),           // Concurrent downloads
    weeks: Some(WeekRange::AllWeeks), // GPS weeks to process
    process_files: Some(true),       // Enable SP3 processing
    recurring: Some(300)             // Interval in seconds
}
```

#### Week Range Options

- `AllWeeks`: All weeks from GPS week 2238+
- `RecentWeeks(n)`: Most recent n weeks
- `WeekRange(start, end)`: Specific range
- `SpecificWeeks(vec)`: Specific weeks only

## Deployment

The system runs two HTTP servers:

- **Ground Control API**: `0.0.0.0:3010` (REST endpoints) + `0.0.0.0:9080` (Restate workflows)
- **CDDIS Archiver**: `0.0.0.0:9080` (Restate workflows)

Both services require environment variables for authentication and cloud storage access.
