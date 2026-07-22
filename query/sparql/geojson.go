package sparql

import "encoding/json"

type FeatureCollection struct {
	Type     string    `json:"type"`
	Features []Feature `json:"features"`
}

type Feature struct {
	Type       string            `json:"type"`
	Geometry   json.RawMessage   `json:"geometry"`
	Properties map[string]string `json:"properties"`
}
