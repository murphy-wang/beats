/*
Package status fetches MySQL server status metrics.

For more information on the query it uses, see:
http://dev.mysql.com/doc/refman/5.7/en/show-status.html
*/
package slave

import (
	"database/sql"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/metricbeat/mb"
	"github.com/elastic/beats/metricbeat/module/mysql"

	"github.com/pkg/errors"
)

var (
	debugf = logp.MakeDebug("mysql-slave")
)

func init() {
	if err := mb.Registry.AddMetricSet("mysql", "slave", New, mysql.ParseDSN); err != nil {
		panic(err)
	}
}

// MetricSet for fetching MySQL server status.
type MetricSet struct {
	mb.BaseMetricSet
	db *sql.DB
}

// New creates and returns a new MetricSet instance.
func New(base mb.BaseMetricSet) (mb.MetricSet, error) {
	return &MetricSet{BaseMetricSet: base}, nil
}

// Fetch fetches status messages from a mysql host.
func (m *MetricSet) Fetch() (common.MapStr, error) {
	if m.db == nil {
		var err error
		m.db, err = mysql.NewDB(m.HostData().URI)
		if err != nil {
			return nil, errors.Wrap(err, "mysql-slave fetch failed")
		}
	}

	status, err := m.loadStatus(m.db)
	if err != nil {
		return nil, err
	}

	event := eventMapping(status)

	if m.Module().Config().Raw {
		event["raw"] = rawEventMapping(status)
	}
	return event, nil
}

// loadStatus loads all status entries from the given database into an array.
func (m *MetricSet) loadStatus(db *sql.DB) (map[string]string, error) {
	// Returns the global status, also for versions previous 5.0.2
	rows, err := db.Query("SHOW SLAVE STATUS;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	mysqlStatus := map[string]string{}

	for rows.Next() {
		var Slave_IO_State sql.NullString
		var Master_Host sql.NullString
		var Master_User sql.NullString
		var Master_Port sql.NullString
		var Connect_Retry sql.NullString
		var Master_Log_File sql.NullString
		var Read_Master_Log_Pos sql.NullString
		var Relay_Log_File sql.NullString
		var Relay_Log_Pos sql.NullString
		var Relay_Master_Log_File sql.NullString
		var Slave_IO_Running sql.NullString
		var Slave_SQL_Running sql.NullString
		var Replicate_Do_DB sql.NullString
		var Replicate_Ignore_DB sql.NullString
		var Replicate_Do_Table sql.NullString
		var Replicate_Ignore_Table sql.NullString
		var Replicate_Wild_Do_Table sql.NullString
		var Replicate_Wild_Ignore_Table sql.NullString
		var Last_Errno sql.NullString
		var Last_Error sql.NullString
		var Skip_Counter sql.NullString
		var Exec_Master_Log_Pos sql.NullString
		var Relay_Log_Space sql.NullString
		var Until_Condition sql.NullString
		var Until_Log_File sql.NullString
		var Until_Log_Pos sql.NullString
		var Master_SSL_Allowed sql.NullString
		var Master_SSL_CA_File sql.NullString
		var Master_SSL_CA_Path sql.NullString
		var Master_SSL_Cert sql.NullString
		var Master_SSL_Cipher sql.NullString
		var Master_SSL_Key sql.NullString
		var Seconds_Behind_Master sql.NullString
		var Master_SSL_Verify_Server_Cert sql.NullString
		var Last_IO_Errno sql.NullString
		var Last_IO_Error sql.NullString
		var Last_SQL_Errno sql.NullString
		var Last_SQL_Error sql.NullString
		var Replicate_Ignore_Server_Ids sql.NullString
		var Master_Server_Id sql.NullString
		var Master_UUID sql.NullString
		var Master_Info_File sql.NullString
		var SQL_Delay sql.NullString
		var SQL_Remaining_Delay sql.NullString
		var Slave_SQL_Running_State sql.NullString
		var Master_Retry_Count sql.NullString
		var Master_Bind sql.NullString
		var Last_IO_Error_Timestamp sql.NullString
		var Last_SQL_Error_Timestamp sql.NullString
		var Master_SSL_Crl sql.NullString
		var Master_SSL_Crlpath sql.NullString
		var Retrieved_Gtid_Set sql.NullString
		var Executed_Gtid_Set sql.NullString
		var Auto_Position sql.NullString
		var Replicate_Rewrite_DB sql.NullString
		var Channel_Name sql.NullString
		var Master_TLS_Version sql.NullString

		err = rows.Scan(&Slave_IO_State, &Master_Host, &Master_User, &Master_Port, &Connect_Retry, &Master_Log_File, &Read_Master_Log_Pos, &Relay_Log_File, &Relay_Log_Pos, &Relay_Master_Log_File, &Slave_IO_Running, &Slave_SQL_Running, &Replicate_Do_DB, &Replicate_Ignore_DB, &Replicate_Do_Table, &Replicate_Ignore_Table, &Replicate_Wild_Do_Table, &Replicate_Wild_Ignore_Table, &Last_Errno, &Last_Error, &Skip_Counter, &Exec_Master_Log_Pos, &Relay_Log_Space, &Until_Condition, &Until_Log_File, &Until_Log_Pos, &Master_SSL_Allowed, &Master_SSL_CA_File, &Master_SSL_CA_Path, &Master_SSL_Cert, &Master_SSL_Cipher, &Master_SSL_Key, &Seconds_Behind_Master, &Master_SSL_Verify_Server_Cert, &Last_IO_Errno, &Last_IO_Error, &Last_SQL_Errno, &Last_SQL_Error, &Replicate_Ignore_Server_Ids, &Master_Server_Id, &Master_UUID, &Master_Info_File, &SQL_Delay, &SQL_Remaining_Delay, &Slave_SQL_Running_State, &Master_Retry_Count, &Master_Bind, &Last_IO_Error_Timestamp, &Last_SQL_Error_Timestamp, &Master_SSL_Crl, &Master_SSL_Crlpath, &Retrieved_Gtid_Set, &Executed_Gtid_Set, &Auto_Position, &Replicate_Rewrite_DB, &Channel_Name, &Master_TLS_Version)
		if err != nil {
			return nil, err
		}

		mysqlStatus["Slave_IO_State"] = Slave_IO_State.String
		mysqlStatus["Master_Host"] = Master_Host.String
		mysqlStatus["Master_User"] = Master_User.String
		mysqlStatus["Master_Port"] = Master_Port.String
		mysqlStatus["Connect_Retry"] = Connect_Retry.String
		mysqlStatus["Master_Log_File"] = Master_Log_File.String
		mysqlStatus["Read_Master_Log_Pos"] = Read_Master_Log_Pos.String
		mysqlStatus["Relay_Log_File"] = Relay_Log_File.String
		mysqlStatus["Relay_Log_Pos"] = Relay_Log_Pos.String
		mysqlStatus["Relay_Master_Log_File"] = Relay_Master_Log_File.String
		mysqlStatus["Slave_IO_Running"] = Slave_IO_Running.String
		mysqlStatus["Slave_SQL_Running"] = Slave_SQL_Running.String
		mysqlStatus["Replicate_Do_DB"] = Replicate_Do_DB.String
		mysqlStatus["Replicate_Ignore_DB"] = Replicate_Ignore_DB.String
		mysqlStatus["Replicate_Do_Table"] = Replicate_Do_Table.String
		mysqlStatus["Replicate_Ignore_Table"] = Replicate_Ignore_Table.String
		mysqlStatus["Replicate_Wild_Do_Table"] = Replicate_Wild_Do_Table.String
		mysqlStatus["Replicate_Wild_Ignore_Table"] = Replicate_Wild_Ignore_Table.String
		mysqlStatus["Last_Errno"] = Last_Errno.String
		mysqlStatus["Last_Error"] = Last_Error.String
		mysqlStatus["Skip_Counter"] = Skip_Counter.String
		mysqlStatus["Exec_Master_Log_Pos"] = Exec_Master_Log_Pos.String
		mysqlStatus["Relay_Log_Space"] = Relay_Log_Space.String
		mysqlStatus["Until_Condition"] = Until_Condition.String
		mysqlStatus["Until_Log_File"] = Until_Log_File.String
		mysqlStatus["Until_Log_Pos"] = Until_Log_Pos.String
		mysqlStatus["Master_SSL_Allowed"] = Master_SSL_Allowed.String
		mysqlStatus["Master_SSL_CA_File"] = Master_SSL_CA_File.String
		mysqlStatus["Master_SSL_CA_Path"] = Master_SSL_CA_Path.String
		mysqlStatus["Master_SSL_Cert"] = Master_SSL_Cert.String
		mysqlStatus["Master_SSL_Cipher"] = Master_SSL_Cipher.String
		mysqlStatus["Master_SSL_Key"] = Master_SSL_Key.String
		mysqlStatus["Seconds_Behind_Master"] = Seconds_Behind_Master.String
		mysqlStatus["Master_SSL_Verify_Server_Cert"] = Master_SSL_Verify_Server_Cert.String
		mysqlStatus["Last_IO_Errno"] = Last_IO_Errno.String
		mysqlStatus["Last_IO_Error"] = Last_IO_Error.String
		mysqlStatus["Last_SQL_Errno"] = Last_SQL_Errno.String
		mysqlStatus["Last_SQL_Error"] = Last_SQL_Error.String
		mysqlStatus["Replicate_Ignore_Server_Ids"] = Replicate_Ignore_Server_Ids.String
		mysqlStatus["Master_Server_Id"] = Master_Server_Id.String
		mysqlStatus["Master_UUID"] = Master_UUID.String
		mysqlStatus["Master_Info_File"] = Master_Info_File.String
		mysqlStatus["SQL_Delay"] = SQL_Delay.String
		mysqlStatus["SQL_Remaining_Delay"] = SQL_Remaining_Delay.String
		mysqlStatus["Slave_SQL_Running_State"] = Slave_SQL_Running_State.String
		mysqlStatus["Master_Retry_Count"] = Master_Retry_Count.String
		mysqlStatus["Master_Bind"] = Master_Bind.String
		mysqlStatus["Last_IO_Error_Timestamp"] = Last_IO_Error_Timestamp.String
		mysqlStatus["Last_SQL_Error_Timestamp"] = Last_SQL_Error_Timestamp.String
		mysqlStatus["Master_SSL_Crl"] = Master_SSL_Crl.String
		mysqlStatus["Master_SSL_Crlpath"] = Master_SSL_Crlpath.String
		mysqlStatus["Retrieved_Gtid_Set"] = Retrieved_Gtid_Set.String
		mysqlStatus["Executed_Gtid_Set"] = Executed_Gtid_Set.String
		mysqlStatus["Auto_Position"] = Auto_Position.String
		mysqlStatus["Replicate_Rewrite_DB"] = Replicate_Rewrite_DB.String
		mysqlStatus["Channel_Name"] = Channel_Name.String
		mysqlStatus["Master_TLS_Version"] = Master_TLS_Version.String

	}

	return mysqlStatus, nil
}
