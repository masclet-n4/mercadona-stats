package pocketbase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	BaseURL    string
	http       *http.Client
	adminToken string
}

type PBTime time.Time

func (t PBTime) Time() time.Time { return time.Time(t) }

func (t *PBTime) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	if s == "null" || s == "" {
		return nil
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.000Z",
		"2006-01-02 15:04:05Z",
	} {
		if parsed, err := time.Parse(layout, s); err == nil {
			*(*time.Time)(t) = parsed
			return nil
		}
	}
	return fmt.Errorf("pocketbase: cannot parse %q as time", s)
}

type listResponse[T any] struct {
	Items []T `json:"items"`
}

type PriceRecord struct {
	ID            string  `json:"id"`
	ProductoID    string  `json:"producto_id"`
	UnitPrice     float64 `json:"unit_price"`
	BulkPrice     float64 `json:"bulk_price"`
	EnOferta      bool    `json:"en_oferta"`
	IsNew         bool    `json:"is_new"`
	FechaMuestreo PBTime  `json:"fecha_muestreo"`
	UnitSize      float64 `json:"unit_size"`
	SizeFormat    string  `json:"size_format"`
}

type Product struct {
	ID string `json:"id"`
}

type PriceChanges struct {
	D6   float64 `json:"d6"`
	D30  float64 `json:"d30"`
	D90  float64 `json:"d90"`
	D180 float64 `json:"d180"`
	D365 float64 `json:"d365"`
}

type PriceChangeCounts struct {
	D6   int `json:"d6"`
	D30  int `json:"d30"`
	D90  int `json:"d90"`
	D180 int `json:"d180"`
	D365 int `json:"d365"`
}

type Variation struct {
	D6   float64 `json:"d6"`
	D30  float64 `json:"d30"`
	D90  float64 `json:"d90"`
	D180 float64 `json:"d180"`
	D365 float64 `json:"d365"`
}

type Stats struct {
	ID                    string            `json:"id,omitempty"`
	ProductID             string            `json:"product_id"`
	Mean                  float64           `json:"mean"`
	PriceChangePercentage PriceChanges      `json:"price_change_percentage"`
	Variation             Variation         `json:"variation"`
	CountPriceChanges     PriceChangeCounts `json:"count_price_changes"`
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// do executes an HTTP request with optional JSON body and admin auth.
// Returns the raw response body and status code.
func (c *Client) do(method, reqURL string, body any) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("pocketbase: marshal: %w", err)
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, reqURL, reader)
	if err != nil {
		return nil, 0, fmt.Errorf("pocketbase: request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.adminToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.adminToken)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("pocketbase: request: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return raw, resp.StatusCode, nil
}

// fetchAll paginates through a PocketBase collection and returns all items.
func fetchAll[T any](c *Client, collection string, extra url.Values) ([]T, error) {
	const perPage = 50
	var all []T

	for page := 1; ; page++ {
		q := url.Values{
			"page":    {fmt.Sprintf("%d", page)},
			"perPage": {fmt.Sprintf("%d", perPage)},
		}
		for k, v := range extra {
			q[k] = v
		}

		raw, status, err := c.do(http.MethodGet,
			fmt.Sprintf("%s/api/collections/%s/records?%s", c.BaseURL, collection, q.Encode()), nil)
		if err != nil {
			return nil, err
		}
		if status >= 300 {
			return nil, fmt.Errorf("pocketbase: fetch %s status %d: %s", collection, status, raw)
		}

		var result listResponse[T]
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, fmt.Errorf("pocketbase: decode %s: %w", collection, err)
		}
		all = append(all, result.Items...)
		if len(result.Items) < perPage {
			break
		}
	}
	return all, nil
}

// --- public API ---

func (c *Client) AuthenticateAdmin(email, password string) error {
	raw, status, err := c.do(http.MethodPost,
		fmt.Sprintf("%s/api/collections/_superusers/auth-with-password", c.BaseURL),
		struct {
			Identity string `json:"identity"`
			Password string `json:"password"`
		}{email, password})
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("pocketbase: admin auth failed (status %d): %s", status, raw)
	}

	var result struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return fmt.Errorf("pocketbase: admin auth decode: %w", err)
	}
	c.adminToken = result.Token
	log.Println("[pocketbase] autenticado como admin")
	return nil
}

func (c *Client) EnsureStatsCollection() error {
	_, status, err := c.do(http.MethodGet,
		fmt.Sprintf("%s/api/collections/mercadona_stats", c.BaseURL), nil)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusOK:
		log.Println("[pocketbase] colección mercadona_stats ya existe")
		return nil
	case http.StatusNotFound:
		log.Println("[pocketbase] creando colección mercadona_stats...")
		return c.createStatsCollection()
	default:
		return fmt.Errorf("pocketbase: check collection status %d", status)
	}
}

func (c *Client) createStatsCollection() error {
	// Resolver collectionId de mercadona_products
	raw, status, err := c.do(http.MethodGet,
		fmt.Sprintf("%s/api/collections/mercadona_products", c.BaseURL), nil)
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("pocketbase: get mercadona_products (status %d): %s", status, raw)
	}

	var col struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &col); err != nil || col.ID == "" {
		return fmt.Errorf("pocketbase: mercadona_products collection id no encontrado")
	}

	// Crear mercadona_stats con relation hacia mercadona_products
	raw, status, err = c.do(http.MethodPost,
		fmt.Sprintf("%s/api/collections", c.BaseURL),
		map[string]any{
			"name": "mercadona_stats",
			"type": "base",
			"fields": []map[string]any{
				{
					"name": "product_id", "type": "relation", "required": true,
					"collectionId": col.ID, "cascadeDelete": false, "maxSelect": 1,
				},
				{"name": "mean", "type": "number"},
				{"name": "price_change_percentage", "type": "json"},
				{"name": "variation", "type": "json"},
				{"name": "count_price_changes", "type": "json"},
			},
		})
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("pocketbase: create collection (status %d): %s", status, raw)
	}
	log.Println("[pocketbase] colección mercadona_stats creada")
	return nil
}

func (c *Client) GetAllProducts() ([]Product, error) {
	return fetchAll[Product](c, "mercadona_products", nil)
}

func (c *Client) GetPriceHistory(productID string, from time.Time) ([]PriceRecord, error) {
	fromStr := from.UTC().Format("2006-01-02 15:04:05Z")
	return fetchAll[PriceRecord](c, "mercadona_prices", url.Values{
		"filter": {fmt.Sprintf("producto_id='%s'&&fecha_muestreo>='%s'", productID, fromStr)},
		"sort":   {"+fecha_muestreo"},
	})
}

func (c *Client) UpsertStats(s *Stats) error {
	method, reqURL := http.MethodPost, fmt.Sprintf("%s/api/collections/mercadona_stats/records", c.BaseURL)
	if s.ID != "" {
		method = http.MethodPatch
		reqURL += "/" + s.ID
	}

	raw, status, err := c.do(method, reqURL, struct {
		ProductID             string            `json:"product_id"`
		Mean                  float64           `json:"mean"`
		PriceChangePercentage PriceChanges      `json:"price_change_percentage"`
		Variation             Variation         `json:"variation"`
		CountPriceChanges     PriceChangeCounts `json:"count_price_changes"`
	}{s.ProductID, s.Mean, s.PriceChangePercentage, s.Variation, s.CountPriceChanges})
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("pocketbase: upsert status %d: %s", status, raw)
	}
	return nil
}

func (c *Client) GetStatsIdsMap() (map[string]string, error) {
	stats, err := fetchAll[Stats](c, "mercadona_stats", nil)
	if err != nil {
		return nil, err
	}
	idMap := make(map[string]string, len(stats))
	for _, s := range stats {
		idMap[s.ProductID] = s.ID
	}
	return idMap, nil
}
