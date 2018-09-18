package beater

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	_ "github.com/go-sql-driver/mysql"

	"github.com/anzot/mysqlbeat/config"
)

// Mysqlbeat configuration.
type Mysqlbeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client

	oldValues    common.MapStr
	oldValuesAge common.MapStr
}

const (
	// query types values
	queryTypeSingleRow    = "single-row"
	queryTypeMultipleRows = "multiple-rows"
	queryTypeTwoColumns   = "two-columns"
	queryTypeSlaveDelay   = "show-slave-delay"

	// special column names values
	columnNameSlaveDelay = "Seconds_Behind_Master"

	// column types values
	columnTypeString = iota
	columnTypeInt
	columnTypeFloat
)

// New creates an instance of mysqlbeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	if len(c.Queries) < 1 {
		return nil, fmt.Errorf("there are no queries to execute")
	}

	safeQueries := true

	logp.Info("Total # of queries to execute: %d", len(c.Queries))

	for i, query := range c.Queries {

		strCleanQuery := strings.TrimSpace(strings.ToUpper(query.SQL))

		if !strings.HasPrefix(strCleanQuery, "SELECT") && !strings.HasPrefix(strCleanQuery, "SHOW") || strings.ContainsAny(strCleanQuery, ";") {
			safeQueries = false
		}

		switch query.Type {
		case
			queryTypeSingleRow,
			queryTypeMultipleRows,
			queryTypeTwoColumns,
			queryTypeSlaveDelay:
		default:
			err := fmt.Errorf("unknown query type: %v", query.Type)
			return nil, err
		}

		logp.Info("Query #%d (type: %s): %s", i, query.Type, query.SQL)
		i++
	}

	if !safeQueries {
		err := fmt.Errorf("only SELECT/SHOW queries are allowed (the char ; is forbidden)")
		return nil, err
	}

	bt := &Mysqlbeat{
		done:         make(chan struct{}),
		config:       c,
		oldValues:    common.MapStr{},
		oldValuesAge: common.MapStr{},
	}
	return bt, nil
}

// Run starts mysqlbeat.
func (bt *Mysqlbeat) Run(b *beat.Beat) error {
	logp.Info("mysqlbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		err := bt.beat(b)
		if err != nil {
			return err
		}
	}
}

// Stop stops mysqlbeat.
func (bt *Mysqlbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Mysqlbeat) beat(b *beat.Beat) error {
	// Build the MySQL connection string
	connString := fmt.Sprintf("%v:%v@tcp(%v:%v)/", bt.config.Username, bt.config.Password, bt.config.Hostname, bt.config.Port)

	db, err := sql.Open("mysql", connString)
	if err != nil {
		return err
	}
	defer db.Close()

	for i, query := range bt.config.Queries {
		events, err := bt.iterateQuery(db, i, query.Type, query.SQL)
		if err != nil {
			return err
		}

		for _, event := range events {
			bt.client.Publish(*event)
		}

		i++
	}

	return nil
}

func (bt *Mysqlbeat) iterateQuery(db *sql.DB, i int, queryType string, queryStr string) ([]*beat.Event, error) {
	// Log the query run time and run the query
	dtNow := time.Now()
	rows, err := db.Query(queryStr)
	if err != nil {
		logp.L().Error("Query #%v error generating event from rows: %v", i, err)
		return nil, err
	}
	defer rows.Close()

	// Populate columns array
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var events []*beat.Event

	switch queryType {
	case queryTypeSingleRow, queryTypeSlaveDelay:
		rows.Next()
		event, err := bt.generateEventFromRow(rows, columns, queryType, dtNow)
		if event != nil {
			events = append(events, event)
		}

		return events, err

	case queryTypeMultipleRows:
		for rows.Next() {
			event, err := bt.generateEventFromRow(rows, columns, queryType, dtNow)

			if err != nil {
				return events, err
			} else if event != nil {
				events = append(events, event)
			}
		}

		return events, err

	case queryTypeTwoColumns:
		event, err := bt.generateEmptyEvent(queryType, dtNow)
		if err != nil {
			return events, err
		}

		for rows.Next() {
			err := bt.appendRowToEvent(event, rows, columns, dtNow)

			if err != nil {
				return events, err
			}
		}

		if event != nil {
			events = append(events, event)
		}

		return events, err
	}

	err = fmt.Errorf("unknown query type: %v", queryType)

	return events, err
}

// appendRowToEvent appends the two-column event the current row data
func (bt *Mysqlbeat) appendRowToEvent(event *beat.Event, row *sql.Rows, columns []string, rowAge time.Time) error {

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// Copy the references into such a []interface{} for row.Scan
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Get RawBytes from data
	err := row.Scan(scanArgs...)
	if err != nil {
		return err
	}

	// First column is the name, second is the value
	strColName := string(values[0])
	strColValue := string(values[1])
	strColType := columnTypeString
	strEventColName := strings.Replace(strColName, bt.config.DeltaWildcard, "_PERSECOND", 1)

	// Try to parse the value to an int64
	nColValue, err := strconv.ParseInt(strColValue, 0, 64)
	if err == nil {
		strColType = columnTypeInt
	}

	// Try to parse the value to a float64
	fColValue, err := strconv.ParseFloat(strColValue, 64)
	if err == nil {
		// If it's not already an established int64, set type to float
		if strColType == columnTypeString {
			strColType = columnTypeFloat
		}
	}

	// If the column name ends with the deltaWildcard
	if strings.HasSuffix(strColName, bt.config.DeltaWildcard) {
		var exists bool
		_, exists = bt.oldValues[strColName]

		// If an older value doesn't exist
		if !exists {
			// Save the current value in the oldValues array
			bt.oldValuesAge[strColName] = rowAge

			if strColType == columnTypeString {
				bt.oldValues[strColName] = strColValue
			} else if strColType == columnTypeInt {
				bt.oldValues[strColName] = nColValue
			} else if strColType == columnTypeFloat {
				bt.oldValues[strColName] = fColValue
			}
		} else {
			// If found the old value's age
			if dtOldAge, ok := bt.oldValuesAge[strColName].(time.Time); ok {
				delta := rowAge.Sub(dtOldAge)

				if strColType == columnTypeInt {
					var calcVal int64

					// Get old value
					oldVal, _ := bt.oldValues[strColName].(int64)
					if nColValue > oldVal {
						// Calculate the delta
						devResult := float64(nColValue-oldVal) / float64(delta.Seconds())
						// Round the calculated result back to an int64
						calcVal = roundF2I(devResult, .5)
					} else {
						calcVal = 0
					}

					// Add the delta value to the event
					event.Fields[strEventColName] = calcVal

					// Save current values as old values
					bt.oldValues[strColName] = nColValue
					bt.oldValuesAge[strColName] = rowAge
				} else if strColType == columnTypeFloat {
					var calcVal float64

					// Get old value
					oldVal, _ := bt.oldValues[strColName].(float64)
					if fColValue > oldVal {
						// Calculate the delta
						calcVal = (fColValue - oldVal) / float64(delta.Seconds())
					} else {
						calcVal = 0
					}

					// Add the delta value to the event
					event.Fields[strEventColName] = calcVal

					// Save current values as old values
					bt.oldValues[strColName] = fColValue
					bt.oldValuesAge[strColName] = rowAge
				} else {
					event.Fields[strEventColName] = strColValue
				}
			}
		}
	} else { // Not a delta column, add the value to the event as is
		if strColType == columnTypeString {
			event.Fields[strEventColName] = strColValue
		} else if strColType == columnTypeInt {
			event.Fields[strEventColName] = nColValue
		} else if strColType == columnTypeFloat {
			event.Fields[strEventColName] = fColValue
		}
	}

	// Great success!
	return nil
}

func (bt *Mysqlbeat) generateEmptyEvent(queryType string, rowAge time.Time) (*beat.Event, error) {
	event := &beat.Event{
		Timestamp: rowAge,
		Fields: common.MapStr{
			"type": queryType,
		},
	}

	return event, nil
}

// generateEventFromRow creates a new event from the row data and returns it
func (bt *Mysqlbeat) generateEventFromRow(row *sql.Rows, columns []string, queryType string, rowAge time.Time) (*beat.Event, error) {
	event, err := bt.generateEmptyEvent(queryType, rowAge)
	if err != nil {
		return nil, err
	}

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// Copy the references into such a []interface{} for row.Scan
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Get RawBytes from data
	err = row.Scan(scanArgs...)
	if err != nil {
		return nil, err
	}

	// Loop on all columns
	for i, col := range values {
		// Get column name and string value
		strColName := string(columns[i])
		strColValue := string(col)
		strColType := columnTypeString

		// Skip column processing when query type is show-slave-delay and the column isn't Seconds_Behind_Master
		if queryType == queryTypeSlaveDelay && strColName != columnNameSlaveDelay {
			continue
		}

		// Set the event column name to the original column name (as default)
		strEventColName := strColName

		// Remove unneeded suffix, add _PERSECOND to calculated columns
		if strings.HasSuffix(strColName, bt.config.DeltaKeyWildcard) {
			strEventColName = strings.Replace(strColName, bt.config.DeltaKeyWildcard, "", 1)
		} else if strings.HasSuffix(strColName, bt.config.DeltaWildcard) {
			strEventColName = strings.Replace(strColName, bt.config.DeltaWildcard, "_PERSECOND", 1)
		}

		// Try to parse the value to an int64
		nColValue, err := strconv.ParseInt(strColValue, 0, 64)
		if err == nil {
			strColType = columnTypeInt
		}

		// Try to parse the value to a float64
		fColValue, err := strconv.ParseFloat(strColValue, 64)
		if err == nil {
			// If it's not already an established int64, set type to float
			if strColType == columnTypeString {
				strColType = columnTypeFloat
			}
		}

		// If the column name ends with the deltaWildcard
		if (queryType == queryTypeSingleRow || queryType == queryTypeMultipleRows) && strings.HasSuffix(strColName, bt.config.DeltaWildcard) {

			var strKey string

			// Get unique row key, if it's a single row - use the column name
			if queryType == queryTypeSingleRow {
				strKey = strColName
			} else if queryType == queryTypeMultipleRows {

				// If the query has multiple rows, a unique row key must be defind using the delta key wildcard and the column name
				strKey, err = getKeyFromRow(bt, values, columns)
				if err != nil {
					return nil, err
				}

				strKey += strColName
			}

			var exists bool
			_, exists = bt.oldValues[strKey]

			// If an older value doesn't exist
			if !exists {
				// Save the current value in the oldValues array
				bt.oldValuesAge[strKey] = rowAge

				if strColType == columnTypeString {
					bt.oldValues[strKey] = strColValue
				} else if strColType == columnTypeInt {
					bt.oldValues[strKey] = nColValue
				} else if strColType == columnTypeFloat {
					bt.oldValues[strKey] = fColValue
				}
			} else {
				// If found the old value's age
				if dtOldAge, ok := bt.oldValuesAge[strKey].(time.Time); ok {
					delta := rowAge.Sub(dtOldAge)

					if strColType == columnTypeInt {
						var calcVal int64

						// Get old value
						oldVal, _ := bt.oldValues[strKey].(int64)

						if nColValue > oldVal {
							// Calculate the delta
							devResult := float64(nColValue-oldVal) / float64(delta.Seconds())
							// Round the calculated result back to an int64
							calcVal = roundF2I(devResult, .5)
						} else {
							calcVal = 0
						}

						// Add the delta value to the event
						event.Fields[strEventColName] = calcVal

						// Save current values as old values
						bt.oldValues[strKey] = nColValue
						bt.oldValuesAge[strKey] = rowAge
					} else if strColType == columnTypeFloat {
						var calcVal float64
						oldVal, _ := bt.oldValues[strKey].(float64)

						if fColValue > oldVal {
							// Calculate the delta
							calcVal = (fColValue - oldVal) / float64(delta.Seconds())
						} else {
							calcVal = 0
						}

						// Add the delta value to the event
						event.Fields[strEventColName] = calcVal

						// Save current values as old values
						bt.oldValues[strKey] = fColValue
						bt.oldValuesAge[strKey] = rowAge
					} else {
						event.Fields[strEventColName] = strColValue
					}
				}
			}
		} else { // Not a delta column, add the value to the event as is
			if strColType == columnTypeString {
				event.Fields[strEventColName] = strColValue
			} else if strColType == columnTypeInt {
				event.Fields[strEventColName] = nColValue
			} else if strColType == columnTypeFloat {
				event.Fields[strEventColName] = fColValue
			}
		}
	}

	// If the event has no data, set to nil
	if len(event.Fields) == 1 {
		event.Fields = nil
	}

	return event, nil
}

// getKeyFromRow is a function that returns a unique key from row
func getKeyFromRow(bt *Mysqlbeat, values []sql.RawBytes, columns []string) (strKey string, err error) {

	keyFound := false

	// Loop on all columns
	for i, col := range values {
		// Get column name and string value
		if strings.HasSuffix(string(columns[i]), bt.config.DeltaKeyWildcard) {
			strKey += string(col)
			keyFound = true
		}
	}

	if !keyFound {
		err = fmt.Errorf("query type multiple-rows requires at least one delta key column")
	}

	return strKey, err
}

// roundF2I is a function that returns a rounded int64 from a float64
func roundF2I(val float64, roundOn float64) (newVal int64) {
	var round float64

	digit := val
	_, div := math.Modf(digit)
	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}

	return int64(round)
}
