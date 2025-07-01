# Project Overview

This project is a full-stack application for fetching, processing, and visualizing geospatial data, primarily focused on satellite navigation systems like GPS. It is designed as a monorepo containing several interconnected components, each with a specific role in the data pipeline.

## Components

### 1. `cddis-archiver`

*   **Language:** Rust
*   **Purpose:** This service is responsible for fetching satellite data from the NASA CDDIS (Crustal Dynamics Data Information System) archive. It is designed to download and store raw data, likely related to satellite orbits and other GNSS (Global Navigation Satellite System) information.
*   **Key Technologies:**
    *   `reqwest`: For making HTTP requests to the CDDIS archive.
    *   `object_store`: To store the downloaded data, likely in a cloud storage service like AWS S3.
    *   `restate-sdk`: Suggests integration with the Restate framework for building resilient, stateful services.

### 2. `ground-control`

*   **Language:** Rust
*   **Purpose:** This is the core data processing engine of the application. It takes the raw satellite data (e.g., in SP3 format), performs the necessary calculations and transformations, and prepares it for consumption by the frontend or other services.
*   **Key Technologies:**
    *   `arrow`: For efficient, in-memory data processing, well-suited for large numerical datasets.
    *   `sp3`: A library for parsing SP3 files, a standard format for satellite orbit data.
    *   `axum`: A web framework for building APIs, likely used to expose the processed data.

### 3. `ui/geoceiver-ui`

*   **Language:** TypeScript, with Next.js and React
*   **Purpose:** This is the web-based user interface for the project. It provides a visual representation of the processed satellite data, allowing users to interact with it.
*   **Key Technologies:**
    *   `next.js` & `react`: For building the frontend application.
    *   `mapbox-gl` & `react-map-gl`: For rendering interactive maps and visualizing the geospatial data.
    *   `tailwindcss`: For styling the user interface.

## Architecture

The overall architecture follows a data pipeline pattern:

1.  **Data Ingestion:** The `cddis-archiver` service fetches raw data from an external source (NASA CDDIS).
2.  **Data Storage:** The raw data is stored in a durable object store (e.g., AWS S3).
3.  **Data Processing:** The `ground-control` service processes the raw data into a more usable format.
4.  **Data Serving:** The processed data is exposed via an API built with `axum`.
5.  **Visualization:** The `geoceiver-ui` frontend consumes the API and visualizes the data on an interactive map.

The use of the Restate framework across the backend services suggests a focus on building a reliable and fault-tolerant system.
