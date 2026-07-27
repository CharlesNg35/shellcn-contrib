package snowflake

import (
	"cmp"
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// lastHour is the window every live panel aggregates over. Snowflake's metering
// and load views are minute-granular, so a rolling hour is the shortest window
// that reads steadily instead of flickering between empty samples.
const lastHour = "DATEADD('hour', -1, CURRENT_TIMESTAMP())"

func accountMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := snowflakeSession(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	history := strconv.Itoa(s.opts.HistoryWindow)
	return pollStream(rc, client, s.opts.MetricsInterval, func(ctx context.Context) error {
		frame := map[string]any{}
		var failure error
		if rows, err := queryRows(ctx, s, `
SELECT COUNT(*) AS "queries",
       COUNT_IF(EXECUTION_STATUS = 'FAIL') AS "failed",
       COUNT_IF(EXECUTION_STATUS = 'RUNNING') AS "running",
       COALESCE(SUM(BYTES_SCANNED), 0) AS "bytes_scanned",
       COALESCE(SUM(ROWS_PRODUCED), 0) AS "rows_produced",
       COALESCE(AVG(TOTAL_ELAPSED_TIME), 0) AS "avg_elapsed_ms",
       COALESCE(SUM(CREDITS_USED_CLOUD_SERVICES), 0) AS "cloud_credits",
       COALESCE(AVG(PERCENTAGE_SCANNED_FROM_CACHE), 0) * 100 AS "cache_pct"
FROM TABLE(`+informationSchema(s.opts.Database)+`.QUERY_HISTORY(RESULT_LIMIT => `+history+`))
WHERE START_TIME >= `+lastHour, nil); err != nil {
			failure = err
		} else if len(rows) > 0 {
			r := rows[0]
			frame["queries"] = number(r["queries"])
			frame["failed"] = number(r["failed"])
			frame["running"] = number(r["running"])
			frame["bytesScanned"] = number(r["bytes_scanned"])
			frame["rowsProduced"] = number(r["rows_produced"])
			frame["avgElapsedMs"] = round2(number(r["avg_elapsed_ms"]))
			frame["cloudCredits"] = round2(number(r["cloud_credits"]))
			frame["cachePct"] = round2(number(r["cache_pct"]))
		}
		if rows, err := queryRows(ctx, s, `
SELECT COALESCE(SUM(CREDITS_USED), 0) AS "credits",
       COALESCE(SUM(CREDITS_USED_COMPUTE), 0) AS "compute_credits"
FROM TABLE(`+informationSchema(s.opts.Database)+`.WAREHOUSE_METERING_HISTORY(DATE_RANGE_START => `+lastHour+`))`, nil); err != nil {
			failure = cmp.Or(failure, err)
		} else if len(rows) > 0 {
			frame["credits"] = round2(number(rows[0]["credits"]))
			frame["computeCredits"] = round2(number(rows[0]["compute_credits"]))
		}
		if started, err := startedWarehouses(ctx, s); err != nil {
			failure = cmp.Or(failure, err)
		} else {
			frame["warehouses"] = started
		}
		if err := emptyFrameErr(frame, failure); err != nil {
			return err
		}
		return enc.Encode(frame)
	})
}

func warehouseMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := snowflakeSession(rc)
	if err != nil {
		return err
	}
	name, err := safeIdentifier(rc.Param("warehouse"))
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	literal := quoteLiteral(name)
	return pollStream(rc, client, s.opts.MetricsInterval, func(ctx context.Context) error {
		frame := map[string]any{}
		var failure error
		if detail, err := showOne(ctx, s, "SHOW WAREHOUSES LIKE "+literal, warehouseSpec(), `"name"`, name); err != nil {
			failure = err
		} else {
			started, maximum := number(detail["started_clusters"]), number(detail["max_clusters"])
			frame["running"] = number(detail["running"])
			frame["queued"] = number(detail["queued"])
			frame["startedClusters"] = started
			frame["maxClusters"] = maximum
			frame["clusterPct"] = percent(started, maximum)
		}
		if rows, err := queryRows(ctx, s, `
SELECT COALESCE(AVG(AVG_RUNNING), 0) AS "avg_running",
       COALESCE(AVG(AVG_QUEUED_LOAD), 0) AS "avg_queued_load",
       COALESCE(AVG(AVG_QUEUED_PROVISIONING), 0) AS "avg_queued_provisioning",
       COALESCE(AVG(AVG_BLOCKED), 0) AS "avg_blocked"
FROM TABLE(`+informationSchema(s.opts.Database)+`.WAREHOUSE_LOAD_HISTORY(DATE_RANGE_START => `+lastHour+`, WAREHOUSE_NAME => `+literal+`))`, nil); err != nil {
			failure = cmp.Or(failure, err)
		} else if len(rows) > 0 {
			r := rows[0]
			frame["avgRunning"] = round2(number(r["avg_running"]))
			frame["avgQueuedLoad"] = round2(number(r["avg_queued_load"]))
			frame["avgQueuedProvisioning"] = round2(number(r["avg_queued_provisioning"]))
			frame["avgBlocked"] = round2(number(r["avg_blocked"]))
		}
		if rows, err := queryRows(ctx, s, `
SELECT COALESCE(SUM(CREDITS_USED), 0) AS "credits"
FROM TABLE(`+informationSchema(s.opts.Database)+`.WAREHOUSE_METERING_HISTORY(DATE_RANGE_START => `+lastHour+`, WAREHOUSE_NAME => `+literal+`))`, nil); err != nil {
			failure = cmp.Or(failure, err)
		} else if len(rows) > 0 {
			frame["credits"] = round2(number(rows[0]["credits"]))
		}
		if err := emptyFrameErr(frame, failure); err != nil {
			return err
		}
		return enc.Encode(frame)
	})
}

// emptyFrameErr surfaces the first source failure when no source produced a
// value, so a role that cannot read the metering views fails visibly instead of
// leaving the panel blank forever.
func emptyFrameErr(frame map[string]any, failure error) error {
	if len(frame) == 0 && failure != nil {
		return failure
	}
	return nil
}

// startedWarehouses counts running warehouses by reading SHOW WAREHOUSES back
// through RESULT_SCAN, so only the aggregate crosses the wire.
func startedWarehouses(ctx context.Context, s *Session) (float64, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, snowflakeErr(err)
	}
	defer func() { _ = conn.Close() }()
	if err := runCommand(ctx, s, conn, "SHOW WAREHOUSES"); err != nil {
		return 0, err
	}
	rows, err := queryRowsOn(ctx, s, conn, `SELECT COUNT_IF(UPPER("state") = 'STARTED') AS "started" FROM `+resultScanFrom, nil)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, plugin.ErrNotFound
	}
	return number(rows[0]["started"]), nil
}

// pollStream emits a frame immediately and then on every tick. Frames run on a
// context a browser disconnect cancels, so a poll already in flight against
// Snowflake is abandoned rather than left to burn its query timeout.
func pollStream(rc *plugin.RequestContext, client plugin.ClientStream, every time.Duration, frame func(context.Context) error) error {
	ctx, cancel := context.WithCancel(rc.Ctx)
	defer cancel()
	defer context.AfterFunc(client.Context(), cancel)()

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		if err := frame(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
