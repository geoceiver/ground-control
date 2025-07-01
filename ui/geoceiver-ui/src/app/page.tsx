"use client";

import Map, { Source, Layer } from "react-map-gl/mapbox";
import type { LineLayerSpecification } from "react-map-gl/mapbox";
import type { FeatureCollection } from "geojson";
import "mapbox-gl/dist/mapbox-gl.css";

const geojson: FeatureCollection = {
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [-122.4, 37.8],
      },
      properties: { title: "915 Front Street, San Francisco, California" },
    },
  ],
};

const layerStyle: LineLayerSpecification = {
  id: "point",
  type: "line",
  source: "mapbox",
  paint: {
    "line-width": 10,
    "line-color": "#007cbf",
  },
};

export default function Home() {
  return (
    <Map
      projection="globe"
      mapboxAccessToken="pk.eyJ1IjoidXJiYW50cmFjdGlvbiIsImEiOiJjbTJqcWh4ajAwN3FkMnFvdmVsNnRuZzA5In0.MECFs2tGWBpUMyYtVr2MJg"
      initialViewState={{
        longitude: -122.4,
        latitude: 37.8,
        zoom: 14,
      }}
      style={{ width: "100vw", height: "100vh" }}
      mapStyle="mapbox://styles/urbantraction/cm2jqjldc007201pe30h5d4hu"
    >
      <Source id="my-data" type="geojson" data={geojson}>
        <Layer {...layerStyle} />
      </Source>
    </Map>
  );
}
