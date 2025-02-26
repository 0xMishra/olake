package jdbc

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
)

const CDCDeletedAt = "_cdc_deleted_at"
const CDCLSN = "_cdc_lsn"
const CDCUpdatedAt = "_cdc_updated_at"

var CDCColumns = map[string]types.DataType{
	CDCDeletedAt: types.Timestamp,
	CDCLSN:       types.String,
	CDCUpdatedAt: types.Timestamp,
}

// Order by Cursor
func PostgresWithoutState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" ORDER BY %s`, stream.Namespace(), stream.Name(), stream.Cursor())
}

// Order by Cursor
func PostgresWithState(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" where "%s">$1 ORDER BY "%s" ASC NULLS FIRST`, stream.Namespace(), stream.Name(), stream.Cursor(), stream.Cursor())
}

// Order by primary keys
func PostgresFullRefresh(stream protocol.Stream) string {
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" ORDER BY %s`, stream.Namespace(), stream.Name(),
		strings.Join(stream.GetStream().SourceDefinedPrimaryKey.Array(), ", "))
}

func PostgresMinMaxRowCountQuery(stream protocol.Stream, splitColumn string) string {
	return fmt.Sprintf(`
		SELECT 
			MIN(%s) AS min_value, 
			MAX(%s) AS max_value, 
			(SELECT reltuples::bigint 
			 FROM pg_class c 
			 JOIN pg_namespace n ON n.oid = c.relnamespace 
			 WHERE c.relname = '%s' AND n.nspname = '%s'
			) AS approx_row_count
		FROM "%s"."%s";`,
		splitColumn, splitColumn, stream.Name(), stream.Namespace(),
		stream.Namespace(), stream.Name())
}

func BuildSplitScanQuery(stream protocol.Stream, splitColumn string, chunk types.Chunk) string {
	condition := ""
	if chunk.Min != nil && chunk.Max != nil {
		condition = fmt.Sprintf("%s >= %v AND %s <= %v", splitColumn, chunk.Min, splitColumn, chunk.Max)
	} else if chunk.Min != nil {
		condition = fmt.Sprintf("%s >= %v", splitColumn, chunk.Min)
	} else if chunk.Max != nil {
		condition = fmt.Sprintf("%s <= %v", splitColumn, chunk.Max)
	}

	return fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE %s`, stream.Namespace(), stream.Name(), condition)
}
