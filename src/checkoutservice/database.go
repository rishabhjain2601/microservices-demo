package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
	
	pb "github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice/genproto"
	_ "github.com/lib/pq"
)

// OrderRecord represents an order in the database
type OrderRecord struct {
	ID              string    `json:"id"`
	UserID          string    `json:"user_id"`
	Email           string    `json:"email"`
	TotalCost       string    `json:"total_cost"`
	Currency        string    `json:"currency"`
	Items           string    `json:"items"` // JSON string of items
	ShippingAddress string    `json:"shipping_address"` // JSON string of address
	CreatedAt       time.Time `json:"created_at"`
}

// DatabaseService handles order persistence
type DatabaseService struct {
	db *sql.DB
}

// NewDatabaseService creates a new database service
func NewDatabaseService(dbURL string) (*DatabaseService, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, err
	}
	
	if err := db.Ping(); err != nil {
		return nil, err
	}
	
	service := &DatabaseService{db: db}
	if err := service.createTables(); err != nil {
		return nil, err
	}
	
	return service, nil
}

// createTables creates the orders table if it doesn't exist
func (ds *DatabaseService) createTables() error {
	query := `
	CREATE TABLE IF NOT EXISTS orders (
		id VARCHAR(255) PRIMARY KEY,
		user_id VARCHAR(255) NOT NULL,
		email VARCHAR(255) NOT NULL,
		total_cost VARCHAR(50) NOT NULL,
		currency VARCHAR(10) NOT NULL,
		items TEXT NOT NULL,
		shipping_address TEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	
	_, err := ds.db.Exec(query)
	return err
}

// SaveOrder saves an order to the database
func (ds *DatabaseService) SaveOrder(order *OrderRecord) error {
	query := `
	INSERT INTO orders (id, user_id, email, total_cost, currency, items, shipping_address, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	
	_, err := ds.db.Exec(query, 
		order.ID, 
		order.UserID, 
		order.Email, 
		order.TotalCost, 
		order.Currency, 
		order.Items, 
		order.ShippingAddress, 
		order.CreatedAt,
	)
	
	return err
}

// GetOrders retrieves orders for a user
func (ds *DatabaseService) GetOrders(userID string) ([]OrderRecord, error) {
	query := `SELECT id, user_id, email, total_cost, currency, items, shipping_address, created_at 
			  FROM orders WHERE user_id = $1 ORDER BY created_at DESC`
	
	rows, err := ds.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var orders []OrderRecord
	for rows.Next() {
		var order OrderRecord
		err := rows.Scan(
			&order.ID,
			&order.UserID,
			&order.Email,
			&order.TotalCost,
			&order.Currency,
			&order.Items,
			&order.ShippingAddress,
			&order.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}
	
	return orders, nil
}

// moneyToString converts money to string representation
func moneyToString(m *pb.Money) string {
	if m == nil {
		return "0.00"
	}
	return fmt.Sprintf("%.2f", float64(m.GetUnits())+float64(m.GetNanos())/1000000000.0)
}

// prepareOrderForDB converts checkout request to database record
func prepareOrderForDB(req *pb.PlaceOrderRequest, orderID string, totalCost *pb.Money, orderItems []*pb.OrderItem) (*OrderRecord, error) {
	// Convert order items to JSON
	itemsJSON, err := json.Marshal(orderItems)
	if err != nil {
		return nil, err
	}
	
	// Convert address to JSON
	addressJSON, err := json.Marshal(req.GetAddress())
	if err != nil {
		return nil, err
	}
	
	return &OrderRecord{
		ID:              orderID,
		UserID:          req.GetUserId(),
		Email:           req.GetEmail(),
		TotalCost:       moneyToString(totalCost),
		Currency:        totalCost.GetCurrencyCode(),
		Items:           string(itemsJSON),
		ShippingAddress: string(addressJSON),
		CreatedAt:       time.Now(),
	}, nil
}
