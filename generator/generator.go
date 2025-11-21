package generator

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/peterstace/simplefeatures/geom"
)

// GeometryEntity represents a geometry and properties that will
// be stored within the GeoParquet file.
type GeometryEntity struct {
	Geom geom.Geometry

	// super generic for now
	Properties map[string]any

	// will be used to group into Parquet row groups.
	GroupID int
}

type ParquetBase struct {
	Entities []GeometryEntity

	// how many rows (not row groups) per file.
	MaxRowsPerFile int
}

// GenerateGroups will process the Entities and determine the groupings
// (and assign GroupID accordingly)
func (pb *ParquetBase) GenerateGroups(rows []GeoParquetRow) []GeoParquetRowGroup {

	return nil
}

func (pb *ParquetBase) GenerateRows() []GeoParquetRow {
	return nil
}

func (pb *ParquetBase) GenerateMetadata() GeoMetadata {

	return GeoMetadata{}
}

// CreateGeoParquet will generate single GeoParquet file (for now).
// TODO(kpfaulkner) refactor to generate many
func (pb *ParquetBase) CreateGeoParquet(filenamePrefix string) {

	metaData := pb.GenerateMetadata()
	rows := pb.GenerateRows()
	groups := pb.GenerateGroups(rows)

}

func main() {
	// Create sample data with point geometries
	rows := []GeoParquetRow{
		{
			ID:       1,
			Name:     "Location A",
			Geometry: mustWKTToWKB("GEOMETRYCOLLECTION(POLYGON ((-90.05084602951591 35.09160986778002, -90.05570431739551 35.091195559781696, -90.05712360525968 35.0888936343525, -90.05139576495 35.08806409574932, -90.04655497955588 35.087732277946174, -90.04703999120049 35.08542316897193, -90.0398079359914 35.08392945605797, -90.04174738056822 35.089435846171824, -90.04732315432175 35.089556812687164, -90.05084602951591 35.09160986778002)))"),
		},
		{
			ID:       2,
			Name:     "Location B",
			Geometry: mustWKTToWKB("GEOMETRYCOLLECTION(POLYGON ((-90.05084602951591 35.09160986778002, -90.05570431739551 35.091195559781696, -90.05712360525968 35.0888936343525, -90.05139576495 35.08806409574932, -90.04655497955588 35.087732277946174, -90.04703999120049 35.08542316897193, -90.0398079359914 35.08392945605797, -90.04174738056822 35.089435846171824, -90.04732315432175 35.089556812687164, -90.05084602951591 35.09160986778002)))"),
		},
		{
			ID:       3,
			Name:     "Location C",
			Geometry: mustWKTToWKB("GEOMETRYCOLLECTION(POLYGON ((-90.05084602951591 35.09160986778002, -90.05570431739551 35.091195559781696, -90.05712360525968 35.0888936343525, -90.05139576495 35.08806409574932, -90.04655497955588 35.087732277946174, -90.04703999120049 35.08542316897193, -90.0398079359914 35.08392945605797, -90.04174738056822 35.089435846171824, -90.04732315432175 35.089556812687164, -90.05084602951591 35.09160986778002)))"),
		},
	}

	// Create GeoParquet metadata
	geoMeta := GeoMetadata{
		Version:       "1.0.0",
		PrimaryColumn: "geometry",
		Columns: map[string]ColumnInfo{
			"geometry": {
				Encoding:      "WKB",
				GeometryTypes: []string{"Polygon", "GeometryCollection"},
				// no CRS means defaults to WGS84
				//CRS: map[string]interface{}{
				//	"type": "name",
				//	"properties": map[string]string{
				//		"name": "EPSG:4326",
				//	},
				//},
				Bbox: []float64{-122.4194, 34.0523, -73.9352, 40.7749}, // [minx, miny, maxx, maxy]
			},
		},
	}

	// Convert metadata to JSON
	geoMetaJSON, err := json.Marshal(geoMeta)
	if err != nil {
		fmt.Printf("Error marshaling geo metadata: %v\n", err)
		return
	}

	// Create output file
	file, err := os.Create("output.parquet")
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer file.Close()

	// Create schema from the struct
	schema := parquet.SchemaOf(GeoParquetRow{})

	// Approach 3: Using low-level Writer with explicit row group buffers
	// This gives you maximum control over row group creation
	writer := parquet.NewWriter(file,
		schema,
		parquet.KeyValueMetadata("geo", string(geoMetaJSON)),
	)
	defer writer.Close()

	// Create a buffer for the first row group
	buffer1 := parquet.NewGenericBuffer[GeoParquetRow](schema)

	// Write first 2 rows to buffer1
	_, err = buffer1.Write(rows[:2])
	if err != nil {
		fmt.Printf("Error writing to buffer1: %v\n", err)
		return
	}

	// Flush buffer1 to create first row group
	n1, err := writer.WriteRowGroup(buffer1)
	if err != nil {
		fmt.Printf("Error writing row group 1: %v\n", err)
		return
	}
	fmt.Printf("Row group 1: written %d rows\n", n1)

	// Create a buffer for the second row group
	buffer2 := parquet.NewGenericBuffer[GeoParquetRow](schema)

	// Write remaining row to buffer2
	_, err = buffer2.Write(rows[2:])
	if err != nil {
		fmt.Printf("Error writing to buffer2: %v\n", err)
		return
	}

	// Flush buffer2 to create second row group
	n2, err := writer.WriteRowGroup(buffer2)
	if err != nil {
		fmt.Printf("Error writing row group 2: %v\n", err)
		return
	}
	fmt.Printf("Row group 2: written %d rows\n", n2)

	fmt.Println("\nGeoParquet file 'output.parquet' created successfully!")
	fmt.Printf("Total: %d rows across 2 row groups\n", n1+n2)
}

func mustWKTToWKB(wkt string) []byte {
	g, err := geom.UnmarshalWKT(wkt)
	if err != nil {
		panic(err)
	}
	return g.AsBinary()
}
