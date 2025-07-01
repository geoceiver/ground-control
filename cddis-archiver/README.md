# CDDIS Archiver

A high-performance Rust application for archiving GNSS data from NASA's Crustal Dynamics Data Information System (CDDIS) using Restate workflows for data processing and Cloudflare R2 for storage.

## Overview

The CDDIS Archiver automatically downloads, validates, and stores GNSS products from CDDIS to cloud storage (R2). It uses workflow orchestration to ensure reliable processing with automatic retries, state management, and parallel processing capabilities.

## Architecture

The application is built using:

- **Restate SDK**: Workflow orchestration and durable execution
- **Tokio**: Async runtime for high-performance I/O
- **Object Store**: Cloud storage abstraction (AWS S3/Cloudflare R2)
- **Ground Control**: Internal library for GNSS data processing

### Core Components

- **`archiver.rs`**: Main workflow orchestration and directory comparison logic
- **`cddis.rs`**: CDDIS API client for fetching file listings and data
- **`queue.rs`**: File processing queue with error handling and retry logic
- **`r2.rs`**: Cloud storage operations and metadata management
- **`utils.rs`**: Shared utilities and HTTP client configuration

## Features

- **Automated Archiving**: Continuously monitors and archives new GNSS products
- **Incremental Processing**: Only downloads files that have changed or are missing
- **Parallel Processing**: Configurable parallelism for efficient data transfer
- **Hash Validation**: Verifies file integrity using SHA512 checksums
- **Error Recovery**: Automatic retries with exponential backoff
- **Recurring Jobs**: Scheduled archiving with configurable intervals
- **File Processing**: Optional SP3 file processing for GNSS data analysis

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```bash
# CDDIS Authentication
EARTHDATA_TOKEN=your_earthdata_token

# Cloud Storage (R2/S3)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_ENDPOINT=your_r2_endpoint
AWS_REGION=auto
BUCKET_NAME=your_bucket_name
```

### Archive Request Configuration

```rust
CDDISArchiveRequest {
    request_id: "unique_request_id",
    parallelism: Some(25),                    // Number of parallel downloads
    weeks: Some(CDDISArchiveRequestWeekRange::AllWeeks), // Week range to process
    process_files: Some(true),                // Enable SP3 file processing
    recurring: Some(300)                      // Recurring interval in seconds
}
```

## Usage

### Running the Service

```bash
# Install dependencies
cargo build --release

# Run the service
cargo run
```

The service starts an HTTP server on `0.0.0.0:9080` and exposes Restate workflow endpoints.

### Week Range Options

- `AllWeeks`: Process all available weeks from GPS week 2238 onwards
- `RecentWeeks(n)`: Process the most recent n weeks
- `WeekRange(start, end)`: Process specific week range
- `SpecificWeeks(vec)`: Process specific weeks only

### Testing

```bash
# Run unit tests
cargo test

# Run integration tests
cargo test --test full_archive_test
cargo test --test queue_test
```

## File Structure

```
src/
├── main.rs          # Application entry point and main workflow
├── lib.rs           # Library exports
├── archiver.rs      # Core archiving workflows and data structures
├── cddis.rs         # CDDIS API client and file operations
├── queue.rs         # File processing queue and error handling
├── r2.rs            # Cloud storage operations
└── utils.rs         # Shared utilities

tests/
├── full_archive_test.rs  # Integration tests
└── queue_test.rs         # Queue processing tests
```
