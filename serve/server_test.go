package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	salsparql "github.com/cgs-earth/sal/query/sparql"
	"github.com/stretchr/testify/require"
)

type endpointRunner struct {
	result salsparql.Result
	err    error
	query  string
}

func (r *endpointRunner) Run(_ context.Context, query string) (salsparql.Result, error) {
	r.query = query
	return r.result, r.err
}

type endpointGeometryRunner struct {
	collection salsparql.FeatureCollection
	err        error
	limit      int
	offset     int
}

func (r *endpointGeometryRunner) Geometries(_ context.Context, limit int, offset int) (salsparql.FeatureCollection, error) {
	r.limit = limit
	r.offset = offset
	return r.collection, r.err
}

func TestEndpointAcceptsGETQueryAndReturnsSPARQLJSON(t *testing.T) {
	runner := &endpointRunner{result: salsparql.Result{
		Header: []string{"s", "name"},
		Rows: [][]string{
			{"https://example.org/alice", "Alice"},
		},
	}}
	server := httptest.NewServer(NewEndpoint(runner))
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/sparql?query=SELECT+%3Fs+WHERE+%7B+%3Fs+%3Fp+%3Fo+%7D", nil)
	require.NoError(t, err)
	req.Header.Set("Accept", sparqlResultsJSON)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, sparqlResultsJSON, resp.Header.Get("Content-Type"))
	require.Equal(t, "SELECT ?s WHERE { ?s ?p ?o }", runner.query)

	var body sparqlJSONResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, []string{"s", "name"}, body.Head.Vars)
	require.Equal(t, "uri", body.Results.Bindings[0]["s"].Type)
	require.Equal(t, "https://example.org/alice", body.Results.Bindings[0]["s"].Value)
	require.Equal(t, "literal", body.Results.Bindings[0]["name"].Type)
	require.Equal(t, "Alice", body.Results.Bindings[0]["name"].Value)
}

func TestEndpointWithMapServesMapAtRoot(t *testing.T) {
	server := httptest.NewServer(NewEndpointWithMap(&endpointRunner{}, &endpointGeometryRunner{}))
	defer server.Close()

	resp, err := http.Get(server.URL + "/")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/html")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "maplibre-gl")
	require.Contains(t, string(body), "tiles.openfreemap.org")
	require.Contains(t, string(body), "/geometries?limit=")
}

func TestEndpointWithMapReturnsGeometryFeatureCollection(t *testing.T) {
	geometryRunner := &endpointGeometryRunner{collection: salsparql.FeatureCollection{
		Type: "FeatureCollection",
		Features: []salsparql.Feature{
			{
				Type:     "Feature",
				Geometry: json.RawMessage(`{"type":"Point","coordinates":[-77.0365,38.8977]}`),
				Properties: map[string]string{
					"subject": "https://example.org/place",
				},
			},
		},
	}}
	server := httptest.NewServer(NewEndpointWithMap(&endpointRunner{}, geometryRunner))
	defer server.Close()

	resp, err := http.Get(server.URL + "/geometries?limit=500&offset=12")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/geo+json", resp.Header.Get("Content-Type"))
	require.Equal(t, 100, geometryRunner.limit)
	require.Equal(t, 12, geometryRunner.offset)

	var body salsparql.FeatureCollection
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "FeatureCollection", body.Type)
	require.Len(t, body.Features, 1)
	require.JSONEq(t, `{"type":"Point","coordinates":[-77.0365,38.8977]}`, string(body.Features[0].Geometry))
	require.Equal(t, "https://example.org/place", body.Features[0].Properties["subject"])
}

func TestEndpointAcceptsFormPOSTQuery(t *testing.T) {
	runner := &endpointRunner{result: salsparql.Result{Header: []string{"s"}}}
	server := httptest.NewServer(NewEndpoint(runner))
	defer server.Close()

	resp, err := http.Post(
		server.URL+"/sparql",
		"application/x-www-form-urlencoded",
		strings.NewReader("query=SELECT+%3Fs+WHERE+%7B+%3Fs+%3Fp+%3Fo+%7D"),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "SELECT ?s WHERE { ?s ?p ?o }", runner.query)
}

func TestEndpointAcceptsSPARQLQueryPOSTBody(t *testing.T) {
	runner := &endpointRunner{result: salsparql.Result{Header: []string{"s"}}}
	server := httptest.NewServer(NewEndpoint(runner))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/sparql", strings.NewReader("SELECT ?s WHERE { ?s ?p ?o }"))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/sparql-query")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "SELECT ?s WHERE { ?s ?p ?o }", runner.query)
}

func TestEndpointRejectsUnsupportedAcceptHeader(t *testing.T) {
	server := httptest.NewServer(NewEndpoint(&endpointRunner{}))
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/sparql?query=SELECT+%3Fs+WHERE+%7B%7D", nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "text/turtle")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	require.Equal(t, http.StatusNotAcceptable, resp.StatusCode)
}

func TestEndpointRejectsMissingQuery(t *testing.T) {
	server := httptest.NewServer(NewEndpoint(&endpointRunner{}))
	defer server.Close()

	resp, err := http.Get(server.URL + "/sparql")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEndpointRejectsUnsupportedPOSTMediaType(t *testing.T) {
	server := httptest.NewServer(NewEndpoint(&endpointRunner{}))
	defer server.Close()

	resp, err := http.Post(server.URL+"/sparql", "application/json", strings.NewReader(`{"query":"SELECT ?s WHERE {}"}`))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	require.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
}

func TestEndpointReturnsBadRequestForSPARQLError(t *testing.T) {
	server := httptest.NewServer(NewEndpoint(&endpointRunner{err: fmt.Errorf("parse SPARQL query")}))
	defer server.Close()

	resp, err := http.Get(server.URL + "/sparql?query=ASK+%7B%7D")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
