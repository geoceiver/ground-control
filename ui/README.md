# Ground Control UI

Interactive web interfaces for visualizing GNSS satellite data and system status.

## Satellite Map

**File:** `satellite-map.html`

An interactive satellite visualization interface displays real-time GNSS satellite positions on a 3D globe.

### Usage

1. **Prerequisites**: Ensure the Ground Control API is running on `localhost:3010`

2. **Open the Interface**: Open `satellite-map.html` in a web browser

3. **Configure Data Source**:

   - Default source is `cddis_cod_ult` (CDDIS COD Ultra-Rapid)
   - Modify the source field to use different data sources

4. **Navigate Time**:

   - Use the epoch slider to navigate through different time periods
   - Satellites automatically update as you scrub through time
   - Currently fixed time range, WIP API to find time bounds for current data set

5. **Interact with Satellites**:
   - Click "Load Satellites" or use the time slider to fetch satellite positions
   - Click on any satellite marker to view detailed information and orbit trace
   - Satellite info panel shows constellation, PRN, position, and data source
   - Click on the map to hide the satellite info panel

### API Integration

The interface connects to the Ground Control API using these endpoints:

- `GET /orbits/{source}/{epoch}` - Fetch all satellite positions for a given epoch
- `GET /orbit/{source}/{satellite}/{epoch}` - Fetch detailed orbit trace for a specific satellite

### Configuration

- **Mapbox Token**: Update mapbox token in html file
- **API Endpoint**: Hardcoded to `localhost:3010` - modify for production deployments
- **Data Sources**: Configurable through the source input field

### Technical Details

- **Map Projection**: Globe projection with fog effects for realistic 3D appearance
- **Marker Styling**: Color-coded satellite markers with constellation-specific colors
- **Performance**: Debounced loading for smooth time navigation
- **Error Handling**: User-friendly status messages for API errors and loading states

### Supported Data Sources

Common data sources include:

- `cddis_cod_ult` - COD Ultra-Rapid products
- `cddis_igs_fin` - IGS Final products
- `cddis_igr_rap` - IGS Rapid products

Refer to the main Ground Control API documentation for the complete list of available sources.
