package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func fillMap(cur mongo.Cursor) (resultMap []map[string]string, err error) {

	var results []bson.M
	if err = cur.All(context.TODO(), &results); err != nil {
		return nil, err
	}

	for _, result := range results {
		resultM := make(map[string]string)
		cur.Decode(&result)

		for k, v := range result {
			field, ok := v.(string)
			if ok {
				resultM[k] = field
			} else {
				return nil, fmt.Errorf("Wrong data type: %v, %v\n", k, v)
			}
		}
		resultMap = append(resultMap, resultM)
	}

	return resultMap, nil
}

// bulkWrite applies a batch of write models (insert/update/delete) to a
// single collection in one round-trip, instead of one round-trip per op.
func bulkWrite(db *mongo.Database, table string, models []mongo.WriteModel) (err error) {
	if len(models) == 0 {
		return nil
	}
	coll := db.Collection(table)
	_, err = coll.BulkWrite(context.TODO(), models)
	return err
}

func dbRead(db *mongo.Database, table string, key string,
	fields map[string]string) (results []map[string]string, err error) {

	coll := db.Collection(table)
	filter := bson.D{}
	if key == "" {
		log.Debugf("Find all in table: %s", coll.Name())
	} else {
		filter = bson.D{{"_id", key}}
	}

	cursor, err := coll.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}

	results, err = fillMap(*cursor)
	if err != nil {
		return nil, err
	}

	if _, ok := fields["<all fields>"]; !ok {
		log.Errorf("Unsupported field projection")
	}

	if len(results) == 0 {
		log.Debugf("_id %s does not exist in table %s\n", key, coll.Name())
		return results, nil
	}

	return results, nil
}

func dbScan(db *mongo.Database, table string, startkey string, recordcount int,
	fields map[string]string) (results []map[string]string, err error) {

	coll := db.Collection(table)
	filter := bson.D{{"_id", bson.D{{"$gte", startkey}}}}
	sort := bson.D{{"_id", 1}}

	cursor, err := coll.Find(
		context.TODO(),
		filter,
		options.Find().SetSort(sort),
		options.Find().SetLimit(int64(recordcount)),
	)
	if err != nil {
		return nil, err
	}
	if _, ok := fields["<all fields>"]; !ok {
		log.Errorf("Unsupported field projection")
	}

	results, err = fillMap(*cursor)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func dbDrop(db *mongo.Database, table string) (err error) {
	coll := db.Collection(table)
	err = coll.Drop(context.TODO())

	return
}
