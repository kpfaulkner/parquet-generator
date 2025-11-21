package generator

// GeoParquetRow represents a row in our GeoParquet file
type GeoParquetRow struct {
	ID       int32  `parquet:"id"`
	Name     string `parquet:"name"`
	Geometry []byte `parquet:"geometry"`

	// bbox for this individiaul geometry
	BBox []byte `parquet:"bbox"`
}

type GeoParquetRowGroup struct {
	Rows []GeoParquetRow
}

// GeoMetadata represents the GeoParquet metadata structure.
// There is 1 set of metadata per file. So bounding boxes etc are for the whole file.
type GeoMetadata struct {
	Version       string                `json:"version"`
	PrimaryColumn string                `json:"primary_column"`
	Columns       map[string]ColumnInfo `json:"columns"`
}

type ColumnInfo struct {
	Encoding      string      `json:"encoding"`
	GeometryTypes []string    `json:"geometry_types"`
	CRS           interface{} `json:"crs,omitempty"`
	Bbox          []float64   `json:"bbox,omitempty"`
}
