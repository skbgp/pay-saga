package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/skbgp/saga-platform/common"
)

type orderHandler struct {
	db   *sql.DB
	idem *common.IdempotencyStore
}

type createOrderRequest struct {
	UserID     string `json:"user_id"`
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
	PriceCents int    `json:"price_cents"`
}

type createOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func (h *orderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleCreate(w, r)
	case http.MethodGet:
		h.handleGet(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCreate processes a new order with Idempotency-Key support.
// Order + outbox event are written in the same DB transaction.
func (h *orderHandler) handleCreate(w http.ResponseWriter, r *http.Request) {
	idempKey := r.Header.Get("Idempotency-Key")
	if idempKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Idempotency-Key header is required",
		})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "failed to read request body",
		})
		return
	}
	bodyHash := common.HashBody(body)

	// Check if this key was already used.
	entry, found, err := h.idem.Check(r.Context(), idempKey)
	if err != nil {
		// Redis down. Fail open - accept the request.
		log.Printf("WARN: idempotency check failed (redis down?): %v", err)
	} else if found {
		if entry.BodyHash != bodyHash {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": "Idempotency-Key already used with a different request body",
			})
			return
		}

		// Same key, same body - return cached response.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(entry.StatusCode)
		w.Write([]byte(entry.Response))
		return
	}

	var req createOrderRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid JSON: " + err.Error(),
		})
		return
	}

	if req.UserID == "" || req.ProductID == "" || req.Quantity <= 0 || req.PriceCents <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "user_id, product_id, quantity (>0), and price_cents (>0) are required",
		})
		return
	}

	orderID := fmt.Sprintf("ord_%s", idempKey[:8])
	totalCents := req.PriceCents * req.Quantity

	// Transactional outbox: order + event in same DB transaction.
	err = insertOrderWithOutbox(r.Context(), h.db, orderID, req, totalCents)
	if err != nil {
		log.Printf("ERROR: insert order: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to create order",
		})
		return
	}

	resp := createOrderResponse{
		OrderID: orderID,
		Status:  "PENDING",
		Message: "order created, payment processing will begin shortly",
	}

	respBytes, _ := json.Marshal(resp)
	respStr := string(respBytes)

	if err := h.idem.Store(r.Context(), idempKey, bodyHash, http.StatusCreated, respStr); err != nil {
		log.Printf("WARN: failed to cache idempotency response: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(respBytes)

	log.Printf("order created: id=%s user=%s product=%s qty=%d total=%d",
		orderID, req.UserID, req.ProductID, req.Quantity, totalCents)
}

func (h *orderHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	orderID := r.URL.Path[len("/orders/"):]
	if orderID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "order ID is required in path",
		})
		return
	}

	var status string
	err := h.db.QueryRowContext(r.Context(),
		"SELECT status FROM orders WHERE id = $1", orderID,
	).Scan(&status)

	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "order not found",
		})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "database error",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"order_id": orderID,
		"status":   status,
	})
}

// insertOrderWithOutbox writes order + outbox event in one transaction.
func insertOrderWithOutbox(ctx context.Context, db *sql.DB, orderID string, req createOrderRequest, totalCents int) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO orders (id, user_id, product_id, quantity, total_cents, status)
		VALUES ($1, $2, $3, $4, $5, 'PENDING')
		ON CONFLICT (id) DO NOTHING`,
		orderID, req.UserID, req.ProductID, req.Quantity, totalCents,
	)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"order_id":    orderID,
		"user_id":     req.UserID,
		"product_id":  req.ProductID,
		"quantity":    req.Quantity,
		"total_cents": totalCents,
	})

	_, err = tx.Exec(`
		INSERT INTO outbox (agg_type, agg_id, event_type, payload, status)
		VALUES ('order', $1, 'order.created', $2, 'PENDING')`,
		orderID, payload,
	)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
