package mongodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var log = logrus.New()

const DataPath string = "./ycsb/workData/"

type MongoFollower struct {
	clientThreadNum int
	queryThreadNum  int

	dbName string

	clients []*mongo.Client
}

func NewMongoFollower(clientTNum int, queryTNum int, dbID int) *MongoFollower {
	if clientTNum <= 0 || queryTNum <= 0 {
		err := errors.New("thread number should be positive integer")
		log.Errorf("create MongoDB Follower failed | clientThreadNum: %v, queryThreadNum: %v, err: %v",
			clientTNum, queryTNum, err)
		return nil
	}

	follower := &MongoFollower{
		clientThreadNum: clientTNum,
		queryThreadNum:  queryTNum,
	}

	if dbID == 0 {
		follower.dbName = "ycsb"
	} else {
		follower.dbName = "ycsb" + strconv.Itoa(dbID)
	}

	// build MongoDB clients
	uri := os.Getenv("MONGODB_URI")
	log.Debugf("mongodb url: %v", uri)
	if uri == "" {
		uri = "mongodb://localhost:27017/"
		// log.Fatal("You must set your 'MONGODB_URI' environmental variable.
		//			See\n\t https://www.mongodb.com/docs/drivers/go/current/usage-examples/#environment-variable")
	}

	for i := 0; i < clientTNum; i++ {
		cli, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
		if err != nil {
			log.Errorf("create MongoDB client failed | err: %v", err)
			return nil
		}
		follower.clients = append(follower.clients, cli)
	}

	return follower
}

func (fl *MongoFollower) FollowerAPI(queries []Query) (
	result [][]map[string]string, latency time.Duration, err error) {

	var wg sync.WaitGroup
	mu := &sync.Mutex{}

	queriesPerClient := make([][]Query, fl.clientThreadNum)
	if fl.clientThreadNum == 1 {
		queriesPerClient[0] = queries
	} else {
		for i := 0; i < len(queries); i++ {
			clientToAppend := i % fl.clientThreadNum
			queriesPerClient[clientToAppend] = append(queriesPerClient[clientToAppend], queries[i])
		}
	}

	wg.Add(fl.clientThreadNum)
	for i := 0; i < fl.clientThreadNum; i++ {
		go func(i int) {
			defer wg.Done()

			db := fl.clients[i].Database(fl.dbName)
			resultsPerClient, latPerClient, clientErr := followerClient(db, queriesPerClient[i], fl.queryThreadNum)

			mu.Lock()
			defer mu.Unlock()
			if clientErr != nil {
				// A single failed op (e.g. a duplicate-key error on INSERT
				// against data left over from a previous run) must not take
				// the whole server process down -- log.Fatal here used to
				// call os.Exit, killing every in-flight request on this
				// replica. Surface it through the normal error return
				// instead, same as any other MongoDB error.
				log.Errorf("followerClient failed | err: %v", clientErr)
				if err == nil {
					err = clientErr
				}
				return
			}
			latency += latPerClient
			result = append(result, resultsPerClient...)
		}(i)
	}
	wg.Wait()

	if err != nil {
		return result, latency, err
	}

	latency = time.Duration(float64(latency) / float64(fl.clientThreadNum))
	return result, latency, nil
}

func (fl *MongoFollower) ClearTable(table string) (err error) {

	dropTable := Query{
		Op:     DROP,
		Table:  table,
		Key:    "",
		Values: nil,
	}

	_, _, err = fl.FollowerAPI([]Query{dropTable})

	if err != nil {
		log.Errorf("clear table failed | err: %v", err)
		return
	}
	log.Debugf("table cleared: %s", table)

	return
}

func (fl *MongoFollower) WaitForWritablePrimary(timeout time.Duration) error {
	if fl == nil || len(fl.clients) == 0 {
		return errors.New("mongodb follower is not initialized")
	}

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result := fl.clients[0].Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}})
		cancel()

		if result.Err() == nil {
			var reply struct {
				IsWritablePrimary bool `bson:"isWritablePrimary"`
				IsMaster          bool `bson:"ismaster"`
			}
			if err := result.Decode(&reply); err == nil && (reply.IsWritablePrimary || reply.IsMaster) {
				return nil
			}
		}

		if result.Err() != nil {
			lastErr = result.Err()
		}
		time.Sleep(1 * time.Second)
	}

	if lastErr != nil {
		return fmt.Errorf("timed out waiting for writable primary: %w", lastErr)
	}
	return fmt.Errorf("timed out waiting for writable primary after %s", timeout)
}

func (fl *MongoFollower) PrintTable(table string) (err error) {

	findAll := Query{
		Op:     READ,
		Table:  table,
		Key:    "",
		Values: map[string]string{"<all fields>": ""},
	}
	findAllRes, _, err := fl.FollowerAPI([]Query{findAll})
	if err != nil {
		return
	}

	log.Println("Current entries in table ", table)
	for i, result := range findAllRes[0] {
		if i >= 10 || i >= len(findAllRes[0])-1 {
			break
		}
		log.Printf("========== entry %v ==========\n", i)

		idVal, ok := result["_id"]
		if !ok {
			err = errors.New("User ID does not exist")
			log.Errorf("print table failed | err: %v", err)
			return
		}
		log.Println("_id", "has value", idVal)
		delete(result, "_id")

		for k, v := range result {
			log.Println(k, "has value", v)
		}
	}
	return
}

func (fl *MongoFollower) CleanUp() (err error) {
	err = fl.ClearTable("usertable")
	if err != nil {
		log.Errorf("clear table failed | err: %v", err)
		return
	}

	for _, cli := range fl.clients {
		err = cli.Disconnect(context.TODO())
		if err != nil {
			log.Errorf("disconnect Mongo DB clients failed | err: %v", err)
			return
		}
	}

	return
}

// writeModelFor converts an INSERT/UPDATE/DELETE query into the matching
// mongo.WriteModel, or nil if the query isn't a write op.
func writeModelFor(query Query) mongo.WriteModel {
	switch query.Op {
	case INSERT:
		doc := bson.D{{Key: "_id", Value: query.Key}}
		for f, v := range query.Values {
			doc = append(doc, bson.E{Key: f, Value: v})
		}
		return mongo.NewInsertOneModel().SetDocument(doc)
	case UPDATE:
		var update bson.D
		for f, v := range query.Values {
			update = append(update, bson.E{Key: f, Value: v})
		}
		return mongo.NewUpdateManyModel().
			SetFilter(bson.D{{Key: "_id", Value: query.Key}}).
			SetUpdate(bson.D{{Key: "$set", Value: update}})
	case DELETE:
		filter := bson.D{{Key: "_id", Value: query.Key}}
		if query.Key == "" {
			filter = bson.D{}
		}
		return mongo.NewDeleteManyModel().SetFilter(filter)
	default:
		return nil
	}
}

// placeholderResult mirrors queryHandler's "done" marker for write ops
// that were applied via bulkWrite instead of individually.
func placeholderResult(query Query) []map[string]string {
	switch query.Op {
	case INSERT:
		return []map[string]string{{"INSERT": "done"}}
	case UPDATE:
		return []map[string]string{{"UPDATE": "done"}}
	case DELETE:
		return []map[string]string{{"DELETE": "done"}}
	default:
		return nil
	}
}

func followerClient(db *mongo.Database, queries []Query, qThreadNum int) (
	results [][]map[string]string, latency time.Duration, err error) {

	// Bulk-apply write ops (INSERT/UPDATE/DELETE), grouped by table, in a
	// single round-trip per table instead of one round-trip per query.
	writeModelsByTable := make(map[string][]mongo.WriteModel)
	var readQueries []Query
	for _, query := range queries {
		if model := writeModelFor(query); model != nil {
			writeModelsByTable[query.Table] = append(writeModelsByTable[query.Table], model)
		} else {
			readQueries = append(readQueries, query)
		}
	}

	if len(writeModelsByTable) > 0 {
		startTime := time.Now()
		for table, models := range writeModelsByTable {
			if err = bulkWrite(db, table, models); err != nil {
				return nil, time.Duration(0), err
			}
		}
		// bulkLatency is the cost of ONE round trip covering every write
		// query in this batch -- count it once, not once per query (that
		// previously inflated a mixed read/write batch's reported latency
		// by a factor of however many writes it contained; the write-only
		// early-return below happened to cancel the same over-count via its
		// own division, which is why this only showed up once batches mixed
		// reads and writes, e.g. YCSB Workload A at BATCHSIZE > 1).
		bulkLatency := time.Since(startTime)
		latency += bulkLatency
		for _, query := range queries {
			if result := placeholderResult(query); result != nil {
				results = append(results, result)
			}
		}
	}

	// Reads/scans/drops have no native multi-key batch primitive worth the
	// complexity here, so they still go one round-trip per query.
	if len(readQueries) == 0 {
		return results, latency, nil
	}

	if qThreadNum == 1 {
		// Single thread
		for _, query := range readQueries {
			startTime := time.Now()
			result, err := queryHandler(db, query)
			if err != nil {
				return nil, time.Duration(0), err
			}
			latency += time.Since(startTime)
			results = append(results, result)
		}
	} else {
		// Multi-thread
		var wg sync.WaitGroup
		mu := &sync.Mutex{}

		// concurrent load
		qBuf := make(chan Query, len(readQueries))
		wg.Add(1)
		go func() {
			defer close(qBuf)
			defer wg.Done()
			for _, q := range readQueries {
				qBuf <- q
			}
		}()

		wg.Add(qThreadNum)
		for i := 0; i < qThreadNum; i++ {
			go func() {
				defer wg.Done()
				for query := range qBuf {
					startTime := time.Now()
					result, qErr := queryHandler(db, query)

					mu.Lock()
					if qErr != nil {
						// See FollowerAPI's fix: a single failed op must not
						// os.Exit the whole server process. Surface it
						// through the normal error return instead.
						log.Errorf("queryHandler failed | err: %v", qErr)
						if err == nil {
							err = qErr
						}
						mu.Unlock()
						continue
					}
					latency += time.Since(startTime)
					results = append(results, result)
					mu.Unlock()
				}
			}()
		}

		wg.Wait()
		if err != nil {
			return nil, time.Duration(0), err
		}
	}
	latency = time.Duration(float64(latency) / float64(len(queries)))
	return results, latency, nil
}

func queryHandler(db *mongo.Database, query Query) (opRes []map[string]string, err error) {

	switch query.Op {
	case READ:
		opRes, err = dbRead(db, query.Table, query.Key, query.Values)
		if err != nil {
			return nil, err
		}
		opRes = append(opRes, map[string]string{"READ": "done"})
	case SCAN:
		scanCount, ok := query.Values["<all fields>"]
		if !ok || scanCount == "" {
			return nil, fmt.Errorf("Unexpected scan count")
		}
		recordCount, err := strconv.Atoi(scanCount)
		if err != nil {
			return nil, err
		}

		opRes, err = dbScan(db, query.Table, query.Key, recordCount, query.Values)
		if err != nil {
			return nil, err
		}
		opRes = append(opRes, map[string]string{"SCAN": "done"})
	case DROP:
		dbDrop(db, query.Table)
		opRes = append(opRes, map[string]string{"DROP": "done"})
	default:
		return nil, fmt.Errorf("Unexpected operator: %s\n", strconv.Itoa(query.Op))
	}
	return opRes, nil
}
