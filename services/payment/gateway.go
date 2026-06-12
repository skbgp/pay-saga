package main

import (
	"fmt"
	"math/rand"
	"time"
)

// PaymentResult represents the outcome of a payment attempt.
type PaymentResult struct {
	Success   bool
	Status    string
	Message   string
	LatencyMs int64
}

// Gateway simulates a payment processor (like Stripe or Razorpay).
type Gateway struct {
	FailureRate float64
	PendingRate float64
	MinLatency  time.Duration
	MaxLatency  time.Duration
}

// DefaultGateway returns a gateway with realistic failure rates.
func DefaultGateway() *Gateway {
	return &Gateway{
		FailureRate: 0.02,
		PendingRate: 0.01,
		MinLatency:  50 * time.Millisecond,
		MaxLatency:  2 * time.Second,
	}
}

// Charge attempts to charge the given amount with simulated latency.
func (g *Gateway) Charge(orderID string, amountCents int) PaymentResult {
	start := time.Now()

	latency := g.MinLatency + time.Duration(rand.Int63n(int64(g.MaxLatency-g.MinLatency)))
	time.Sleep(latency)

	elapsed := time.Since(start).Milliseconds()

	roll := rand.Float64()

	if roll < g.FailureRate {
		reasons := []string{
			"card declined",
			"insufficient funds",
			"card expired",
			"fraud suspected",
		}
		reason := reasons[rand.Intn(len(reasons))]

		return PaymentResult{
			Success:   false,
			Status:    "DECLINED",
			Message:   fmt.Sprintf("payment failed: %s (order=%s amount=%d)", reason, orderID, amountCents),
			LatencyMs: elapsed,
		}
	}

	if roll < g.FailureRate+g.PendingRate {
		return PaymentResult{
			Success:   false,
			Status:    "PENDING",
			Message:   fmt.Sprintf("payment requires 3DS verification (order=%s)", orderID),
			LatencyMs: elapsed,
		}
	}

	return PaymentResult{
		Success:   true,
		Status:    "SUCCESS",
		Message:   fmt.Sprintf("payment successful (order=%s amount=%d)", orderID, amountCents),
		LatencyMs: elapsed,
	}
}
