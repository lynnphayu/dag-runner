package repositories

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB handles database operations for the DAG executor
type MongoDB struct {
	client *mongo.Client
	db     *mongo.Database
}

// NewMongoDB creates a new MongoDB repository
func NewMongoDB(uri string, dbName string) (*MongoDB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Verify connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &MongoDB{
		client: client,
		db:     client.Database(dbName),
	}, nil
}

// Close closes the database connection
func (r *MongoDB) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.client.Disconnect(ctx)
}

// Create inserts a new document
func (r *MongoDB) Create(collection string, data map[string]interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := r.db.Collection(collection).InsertOne(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("failed to insert document: %w", err)
	}

	return result.InsertedID, nil
}

// Retrieve fetches documents based on query
func (r *MongoDB) Retrieve(collection string, fields []string, filter map[string]interface{}) ([]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Build projection if specific fields are requested
	projection := bson.M{}
	if len(fields) > 0 && fields[0] != "*" {
		for _, field := range fields {
			projection[field] = 1
		}
	}

	// Convert string _id to ObjectID if present
	if idStr, ok := filter["_id"].(string); ok {
		objectID, err := primitive.ObjectIDFromHex(idStr)
		if err != nil {
			return nil, fmt.Errorf("invalid ObjectID format: %w", err)
		}
		filter["_id"] = objectID
	}

	// Execute find operation
	cursor, err := r.db.Collection(collection).Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return nil, fmt.Errorf("failed to execute find: %w", err)
	}
	defer cursor.Close(ctx)

	var results []interface{}
	if err = cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode results: %w", err)
	}

	return results, nil
}

// Update updates documents based on filter
func (r *MongoDB) Update(collection string, update map[string]interface{}, filter map[string]interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := r.db.Collection(collection).UpdateMany(
		ctx,
		filter,
		bson.M{"$set": update},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to update documents: %w", err)
	}

	return result.ModifiedCount, nil
}

// Delete removes documents based on filter
func (r *MongoDB) Delete(collection string, filter map[string]interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := r.db.Collection(collection).DeleteMany(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to delete documents: %w", err)
	}

	return result.DeletedCount, nil
}

// GetCollectionNames returns all collection names in the database
func (r *MongoDB) GetCollectionNames() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collections, err := r.db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}

	return collections, nil
}
