package driver

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// Simple Full Refresh Sync; Loads table fully
func (p *Postgres) backfill(pool *protocol.WriterPool, stream protocol.Stream) error {
	backfillCtx := context.TODO()
	// check for data distribution
	splitChunks, err := p.splitTableIntoChunks(stream)
	if err != nil {
		return fmt.Errorf("failed to start backfill: %s", err)
	}
	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(splitChunks))
	tx, err := p.client.BeginTx(backfillCtx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}

	defer tx.Rollback()
	processChunk := func(ctx context.Context, chunk types.Chunk, number int) error {
		batchStartTime := time.Now()
		stmt := jdbc.BuildSplitScanQuery(stream, p.config.SplitColumn, chunk)

		setter := jdbc.NewReader(context.TODO(), stmt, p.config.BatchSize, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return tx.Query(query, args...)
		})

		insert, err := pool.NewThread(backfillCtx, stream)
		if err != nil {
			return err
		}
		defer insert.Close()
		err = setter.Capture(func(rows *sql.Rows) error {
			// Create a map to hold column names and values
			record := make(types.Record)

			// Scan the row into the map
			err := utils.MapScan(rows, record)
			if err != nil {
				return fmt.Errorf("failed to mapScan record data: %s", err)
			}

			// generate olake id
			olakeID := utils.GetKeysHash(record, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
			// insert record
			exit, err := insert.Insert(types.CreateRawRecord(olakeID, record, 0))
			if err != nil {
				return err
			}
			if exit {
				return nil
			}
			return nil
		})
		logger.Infof("chunk[%d] completed in %s", number, time.Since(batchStartTime))
		return err
	}
	return utils.Concurrent(backfillCtx, splitChunks, p.config.MaxThreads, processChunk)
}

func (p *Postgres) splitTableIntoChunks(stream protocol.Stream) ([]types.Chunk, error) {
	minMaxRowCountQuery := jdbc.PostgresMinMaxRowCountQuery(stream, p.config.SplitColumn)
	type streamStats struct {
		MinValue, MaxValue interface{}
		ApproxRowCount     int64
	}
	row := p.client.QueryRow(minMaxRowCountQuery)

	var stats streamStats
	err := row.Scan(&stats.MinValue, &stats.MaxValue, &stats.ApproxRowCount)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch table stats: %v", err)
	}
	if stats.MinValue == stats.MaxValue {
		return []types.Chunk{{Min: stats.MinValue, Max: stats.MaxValue}}, nil
	}

	splitEvenlySizedChunks := func(min, max interface{}, approximateRowCnt int64, chunkSize, dynamicChunkSize int) ([]types.Chunk, error) {
		if approximateRowCnt <= int64(chunkSize) {
			return []types.Chunk{{Min: nil, Max: nil}}, nil
		}

		var splits []types.Chunk
		var chunkStart interface{}
		chunkEnd, err := utils.AddConstantToInterface(min, dynamicChunkSize)
		if err != nil {
			return nil, fmt.Errorf("failed to split even size chunks: %s", err)
		}
		for utils.CompareInterfaceValue(chunkEnd, max) <= 0 {
			splits = append(splits, types.Chunk{Min: chunkStart, Max: chunkEnd})
			chunkStart = chunkEnd
			newChunkEnd, err := utils.AddConstantToInterface(chunkEnd, dynamicChunkSize)
			if err != nil {
				return nil, fmt.Errorf("failed to split even size chunks: %s", err)
			}
			chunkEnd = newChunkEnd
		}

		splits = append(splits, types.Chunk{Min: chunkStart, Max: nil})
		return splits, nil
	}

	splitUnevenlySizedChunks := func(tableID string, splitColumn string, min, max interface{}, chunkSize int) ([]types.Chunk, error) {
		var splits []types.Chunk
		var chunkStart interface{}
		splitColumnType, _ := stream.Schema().GetType(splitColumn)
		chunkEnd, err := p.nextChunkEnd(min, tableID, splitColumn, max, splitColumnType)
		if err != nil {
			return nil, fmt.Errorf("failed to split uneven size chunks: %s", err)
		}
		count := 0

		for chunkEnd != nil {
			splits = append(splits, types.Chunk{Min: chunkStart, Max: chunkEnd})
			if count%10 == 0 {
				time.Sleep(1000 * time.Millisecond)
			}
			count++
			chunkStart = chunkEnd
			schemaType, err := stream.Schema().GetType(splitColumn)
			if err != nil {
				return nil, fmt.Errorf("failed to get schema type: %s", err)
			}
			chunkEnd, err = p.nextChunkEnd(chunkEnd, tableID, splitColumn, max, schemaType)
			if err != nil {
				return nil, fmt.Errorf("failed to split uneven size chunks: %s", err)
			}
		}

		splits = append(splits, types.Chunk{Min: chunkStart, Max: nil})
		return splits, nil
	}

	if p.config.SplitColumn != "" {
		splitColumn := p.config.SplitColumn
		_, contains := utils.ArrayContains(stream.GetStream().SourceDefinedPrimaryKey.Array(), func(element string) bool {
			return element == splitColumn
		})
		if !contains {
			return nil, fmt.Errorf("provided split column is not a primary key")
		}
		if p.config.SplitStrategy == "evenly_distributed" {
			// TODO: only some types allowed for evenly distribution
			distributionFactor, err := p.calculateDistributionFactor(stats.MinValue, stats.MaxValue, stats.ApproxRowCount)
			if err != nil {
				return nil, fmt.Errorf("failed to calculate distribution factor: %s", err)
			}
			dynamicChunkSize := int(distributionFactor * float64(p.config.BatchSize))
			if dynamicChunkSize < 1 {
				dynamicChunkSize = 1
			}

			return splitEvenlySizedChunks(stats.MinValue, stats.MaxValue, stats.ApproxRowCount, p.config.BatchSize, dynamicChunkSize)
		}
		return splitUnevenlySizedChunks(stream.GetStream().Name, splitColumn, stats.MinValue, stats.MaxValue, p.config.BatchSize)
	} else {
		// TODO: CTID Approach
		return nil, fmt.Errorf("split strategy not supported")
	}
}

func (p *Postgres) nextChunkEnd(previousChunkEnd interface{}, tableID string, splitColumn string, max interface{}, columnType types.DataType) (interface{}, error) {
	// chunkEnd, err := p.queryNextChunkMax(tableID, splitColumn, chunkSize, previousChunkEnd)
	var chunkEnd interface{}
	query := fmt.Sprintf("SELECT MAX(%s) FROM (SELECT %s FROM %s WHERE %s >= %v ORDER BY %s ASC LIMIT %d) AS T", splitColumn, splitColumn, tableID, splitColumn, previousChunkEnd, splitColumn, p.config.BatchSize)
	err := p.client.QueryRow(query).Scan(&chunkEnd)

	if err != nil {
		return nil, fmt.Errorf("failed to query[%s] next chunk end: %s", query, err)
	}
	if chunkEnd == previousChunkEnd {
		chunkEnd, err = p.queryMin(tableID, splitColumn, chunkEnd)
		if err != nil {
			return nil, err
		}
	}
	// if chunkEnd.(float64) >= max.(float64) {
	// 	return nil, nil
	// }
	return chunkEnd, nil
}

func (p *Postgres) queryMin(tableID string, splitColumn string, excludedLowerBound interface{}) (interface{}, error) {
	query := fmt.Sprintf("SELECT MIN(%s) FROM %s WHERE %s > %v", splitColumn, tableID, splitColumn, excludedLowerBound)
	var minVal interface{}
	err := p.client.QueryRow(query).Scan(&minVal)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query[%s]: %s", query, err)
	}
	return minVal, nil
}

func (p *Postgres) calculateDistributionFactor(min, max interface{}, approximateRowCnt int64) (float64, error) {
	if approximateRowCnt == 0 {
		return float64(^uint(0) >> 1), nil // Return the maximum float64 value
	}
	var minBig, maxBig *big.Float
	switch min := min.(type) {
	case int:
		minBig = big.NewFloat(float64(min))
	case int64:
		minBig = big.NewFloat(float64(min))
	case float32:
		minBig = big.NewFloat(float64(min))
	case float64:
		minBig = big.NewFloat(min)
	}

	switch max := max.(type) {
	case int:
		maxBig = big.NewFloat(float64(max))
	case int64:
		maxBig = big.NewFloat(float64(max))
	case float32:
		maxBig = big.NewFloat(float64(max))
	case float64:
		maxBig = big.NewFloat(max)
	}

	if minBig == nil || maxBig == nil {
		return 0.0, fmt.Errorf("failed to convert min or max value to big.Float")
	}
	difference := new(big.Float).Sub(maxBig, minBig)
	subRowCnt := new(big.Float).Add(difference, big.NewFloat(1))
	approxRowCntBig := new(big.Float).SetInt64(approximateRowCnt)
	distributionFactor := new(big.Float).Quo(subRowCnt, approxRowCntBig)
	factor, _ := distributionFactor.Float64()

	return factor, nil
}

// // Incremental Sync based on a Cursor Value
// func (p *Postgres) incrementalSync(pool *protocol.WriterPool, stream protocol.Stream) error {
// 	incrementalCtx := context.TODO()
// 	tx, err := p.client.BeginTx(incrementalCtx, &sql.TxOptions{
// 		Isolation: sql.LevelRepeatableRead,
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	defer tx.Rollback()

// 	args := []any{}
// 	statement := jdbc.PostgresWithoutState(stream)

// 	// intialState := stream.InitialState()
// 	// if intialState != nil {
// 	// 	logger.Debugf("Using Initial state for stream %s : %v", stream.ID(), intialState)
// 	// 	statement = jdbc.PostgresWithState(stream)
// 	// 	args = append(args, intialState)
// 	// }
// 	insert, err := pool.NewThread(incrementalCtx, stream)
// 	if err != nil {
// 		return err
// 	}
// 	defer insert.Close()
// 	setter := jdbc.NewReader(context.Background(), statement, p.config.BatchSize, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
// 		return tx.Query(query, args...)
// 	}, args...)
// 	return setter.Capture(func(rows *sql.Rows) error {
// 		// Create a map to hold column names and values
// 		record := make(types.Record)

// 		// Scan the row into the map
// 		err := utils.MapScan(rows, record)
// 		if err != nil {
// 			return fmt.Errorf("failed to mapScan record data: %s", err)
// 		}

// 		// create olakeID
// 		olakeID := utils.GetKeysHash(record, stream.GetStream().SourceDefinedPrimaryKey.Array()...)

// 		// insert record
// 		exit, err := insert.Insert(types.CreateRawRecord(olakeID, record, 0))
// 		if err != nil {
// 			return err
// 		}
// 		if exit {
// 			return nil
// 		}
// 		// TODO: Postgres State Management
// 		// err = p.UpdateState(stream, record)
// 		// if err != nil {
// 		// 	return err
// 		// }

// 		return nil
// 	})
// }
