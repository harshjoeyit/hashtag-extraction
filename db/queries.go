package db

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Inc(tag string, delta int) error {
	// Filter to find the document to update
	filter := bson.M{"tag": tag}

	// Define the update operation
	update := bson.M{
		"$inc": bson.M{ // Increment the total_posts field by delta
			"total_posts": delta,
		},
	}

	// Create the document if it doesn't exist
	opts := options.Update().SetUpsert(true)

	// Perform the update
	updateResult, err := collection.UpdateOne(context.TODO(), filter, update, opts)
	if err != nil {
		return err
	}

	_ = updateResult

	// Print the result
	// log.Printf("Matched %v document(s) and updated %v document(s)\n", updateResult.MatchedCount, updateResult.ModifiedCount)

	// if updateResult.UpsertedID != nil {
	// 	log.Printf("New document created with ID: %v\n", updateResult.UpsertedID)
	// }

	return nil
}
