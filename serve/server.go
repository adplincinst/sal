package serve

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	salsparql "github.com/cgs-earth/sal/query/sparql"
)

const sparqlResultsJSON = "application/sparql-results+json"

//go:embed map.html
var mapHTML string

// Serve starts a read-only SPARQL Protocol HTTP endpoint backed by DuckDB.
func Serve(ctx context.Context, addr string, tablePath string, layout salsparql.ObjectLayout, withMap bool) error {
	runner := salsparql.DuckDBRunner{
		TablePath: tablePath,
		Layout:    layout,
	}
	handler := NewEndpoint(runner)
	if withMap {
		handler = NewEndpointWithMap(runner, runner)
	}
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}
	go func() {
		<-ctx.Done()
		if err := server.Shutdown(context.Background()); err != nil {
			slog.Error("failed to stop SPARQL endpoint", "error", err)
		}
	}()
	if withMap {
		fmt.Printf("Serving map at http://localhost%s/ and SPARQL endpoint at http://localhost%s/sparql\n", addr, addr)
	} else {
		fmt.Printf("Serving SPARQL endpoint at http://localhost%s/sparql\n", addr)
	}
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// NewEndpoint returns an HTTP handler for the SPARQL Protocol query operation.
func NewEndpoint(runner salsparql.Runner) http.Handler {
	mux := http.NewServeMux()
	handler := sparqlHandler{runner: runner}
	mux.Handle("/", handler)
	mux.Handle("/sparql", handler)
	return mux
}

// NewEndpointWithMap returns an HTTP handler with a MapLibre UI at / and SPARQL at /sparql.
func NewEndpointWithMap(runner salsparql.Runner, geometryRunner salsparql.GeometryRunner) http.Handler {
	mux := http.NewServeMux()
	sparql := sparqlHandler{runner: runner}
	mux.Handle("/sparql", sparql)
	mux.Handle("/geometries", geometryHandler{runner: geometryRunner})
	mux.HandleFunc("/", mapHandler)
	return mux
}

type sparqlHandler struct {
	runner salsparql.Runner
}

func (h sparqlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type")
	w.Header().Set("Accept-Post", "application/sparql-query, application/x-www-form-urlencoded")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST, OPTIONS")
		http.Error(w, "SPARQL endpoint only supports GET and POST query requests", http.StatusMethodNotAllowed)
		return
	}
	if !acceptsSPARQLJSON(r.Header.Get("Accept")) {
		http.Error(w, "only application/sparql-results+json responses are supported", http.StatusNotAcceptable)
		return
	}

	query, err := queryFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), statusForQueryRequestError(err))
		return
	}
	result, err := h.runner.Run(r.Context(), query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", sparqlResultsJSON)
	if err := json.NewEncoder(w).Encode(sparqlJSONResult(result)); err != nil {
		slog.Error("failed to write SPARQL JSON result", "error", err)
	}
}

type geometryHandler struct {
	runner salsparql.GeometryRunner
}

func (h geometryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Accept")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, OPTIONS")
		http.Error(w, "geometry endpoint only supports GET requests", http.StatusMethodNotAllowed)
		return
	}
	limit := intQueryParam(r, "limit", 100)
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	offset := intQueryParam(r, "offset", 0)
	if offset < 0 {
		offset = 0
	}

	collection, err := h.runner.Geometries(r.Context(), limit, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/geo+json")
	if err := json.NewEncoder(w).Encode(collection); err != nil {
		slog.Error("failed to write GeoJSON result", "error", err)
	}
}

func intQueryParam(r *http.Request, name string, fallback int) int {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func mapHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "map only supports GET requests", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, mapHTML)
}

func queryFromRequest(r *http.Request) (string, error) {
	switch r.Method {
	case http.MethodGet:
		return requiredQuery(r.URL.Query().Get("query"))
	case http.MethodPost:
		contentType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
		switch contentType {
		case "application/sparql-query":
			defer func() {
				if err := r.Body.Close(); err != nil {
					slog.Error("failed to close SPARQL request body", "error", err)
				}
			}()
			b, err := io.ReadAll(r.Body)
			if err != nil {
				return "", fmt.Errorf("read SPARQL query body: %w", err)
			}
			return requiredQuery(string(b))
		case "application/x-www-form-urlencoded", "":
			if err := r.ParseForm(); err != nil {
				return "", fmt.Errorf("parse SPARQL form request: %w", err)
			}
			return requiredQuery(r.Form.Get("query"))
		default:
			return "", errUnsupportedMediaType
		}
	default:
		return "", fmt.Errorf("unsupported method %s", r.Method)
	}
}

var errUnsupportedMediaType = errors.New("POST requests must use application/sparql-query or application/x-www-form-urlencoded")

func requiredQuery(query string) (string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", fmt.Errorf("SPARQL request is missing a query parameter or body")
	}
	return query, nil
}

func statusForQueryRequestError(err error) int {
	if errors.Is(err, errUnsupportedMediaType) {
		return http.StatusUnsupportedMediaType
	}
	return http.StatusBadRequest
}

func acceptsSPARQLJSON(accept string) bool {
	accept = strings.TrimSpace(accept)
	if accept == "" {
		return true
	}
	for _, part := range strings.Split(accept, ",") {
		mediaType := strings.ToLower(strings.TrimSpace(strings.Split(part, ";")[0]))
		switch mediaType {
		case "*/*", "application/*", sparqlResultsJSON, "application/json":
			return true
		}
	}
	return false
}

type sparqlJSONHead struct {
	Vars []string `json:"vars"`
}

type sparqlJSONBinding struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type sparqlJSONResults struct {
	Bindings []map[string]sparqlJSONBinding `json:"bindings"`
}

type sparqlJSONResponse struct {
	Head    sparqlJSONHead    `json:"head"`
	Results sparqlJSONResults `json:"results"`
}

func sparqlJSONResult(result salsparql.Result) sparqlJSONResponse {
	bindings := make([]map[string]sparqlJSONBinding, 0, len(result.Rows))
	for _, row := range result.Rows {
		binding := make(map[string]sparqlJSONBinding)
		for i, name := range result.Header {
			if i >= len(row) {
				continue
			}
			binding[name] = sparqlJSONBinding{
				Type:  sparqlBindingType(row[i]),
				Value: row[i],
			}
		}
		bindings = append(bindings, binding)
	}
	return sparqlJSONResponse{
		Head:    sparqlJSONHead{Vars: result.Header},
		Results: sparqlJSONResults{Bindings: bindings},
	}
}

func sparqlBindingType(value string) string {
	if strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://") {
		return "uri"
	}
	if strings.HasPrefix(value, "_:") {
		return "bnode"
	}
	return "literal"
}
