# Ground Control

A high-performance GNSS data processing and archiving system built with [Restate](https://restate.dev/) and Rust, featuring satellite orbit calculations, real-time position/corrections API, and automated archival of orbit and clock corrections data.

![image](https://github.com/user-attachments/assets/fd6fded3-fd36-4c00-acb8-e4f001e11ae6)


## Overview

Ground Control is a comprehensive GNSS (Global Navigation Satellite System) data processing platform that provides:

- **Real-time satellite position calculations** with precise orbit data
- **Automated GNSS data archiving** from NASA's CDDIS to cloud storage
- **REST API** for satellite position/correction data
- **Workflow orchestration** using Restate for reliable, durable execution

The system consists of two main components:

- **Ground Control**: Core library and API for satellite orbit calculations and SP3 file processing
- **CDDIS Archiver**: Data archival workflow for NASA's CDDIS repository

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

## Web Interface

The project includes an interactive satellite visualization interface:

- **Satellite Map**: `ui/satellite-map.html` - Interactive globe showing real-time satellite positions with constellation colors, orbit traces, and detailed satellite information

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
└── ui/                     # Web interface
    └── satellite-map.html  # Interactive satellite visualization
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
    weeks: Some(CDDISArchiveRequestWeekRange::AllWeeks), // GPS weeks to process
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
