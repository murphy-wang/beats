package slave

import (
	//	"database/sql"
	"github.com/elastic/beats/libbeat/common"
	s "github.com/elastic/beats/metricbeat/schema"
	c "github.com/elastic/beats/metricbeat/schema/mapstrstr"
)

var (
	schema = s.Schema{
		"Slave_IO_State":                c.Str("Slave_IO_State"),
		"Master_Host":                   c.Str("Master_Host"),
		"Master_User":                   c.Str("Master_User"),
		"Master_Port":                   c.Str("Master_Port"),
		"Connect_Retry":                 c.Str("Connect_Retry"),
		"Master_Log_File":               c.Str("Master_Log_File"),
		"Read_Master_Log_Pos":           c.Str("Read_Master_Log_Pos"),
		"Relay_Log_File":                c.Str("Relay_Log_File"),
		"Relay_Log_Pos":                 c.Str("Relay_Log_Pos"),
		"Relay_Master_Log_File":         c.Str("Relay_Master_Log_File"),
		"Slave_IO_Running":              c.Str("Slave_IO_Running"),
		"Slave_SQL_Running":             c.Str("Slave_SQL_Running"),
		"Replicate_Do_DB":               c.Str("Replicate_Do_DB"),
		"Replicate_Ignore_DB":           c.Str("Replicate_Ignore_DB"),
		"Replicate_Do_Table":            c.Str("Replicate_Do_Table"),
		"Replicate_Ignore_Table":        c.Str("Replicate_Ignore_Table"),
		"Replicate_Wild_Do_Table":       c.Str("Replicate_Wild_Do_Table"),
		"Replicate_Wild_Ignore_Table":   c.Str("Replicate_Wild_Ignore_Table"),
		"Last_Errno":                    c.Str("Last_Errno"),
		"Last_Error":                    c.Str("Last_Error"),
		"Skip_Counter":                  c.Str("Skip_Counter"),
		"Exec_Master_Log_Pos":           c.Str("Exec_Master_Log_Pos"),
		"Relay_Log_Space":               c.Str("Relay_Log_Space"),
		"Until_Condition":               c.Str("Until_Condition"),
		"Until_Log_File":                c.Str("Until_Log_File"),
		"Until_Log_Pos":                 c.Str("Until_Log_Pos"),
		"Master_SSL_Allowed":            c.Str("Master_SSL_Allowed"),
		"Master_SSL_CA_File":            c.Str("Master_SSL_CA_File"),
		"Master_SSL_CA_Path":            c.Str("Master_SSL_CA_Path"),
		"Master_SSL_Cert":               c.Str("Master_SSL_Cert"),
		"Master_SSL_Cipher":             c.Str("Master_SSL_Cipher"),
		"Master_SSL_Key":                c.Str("Master_SSL_Key"),
		"Seconds_Behind_Master":         c.Str("Seconds_Behind_Master"),
		"Master_SSL_Verify_Server_Cert": c.Str("Master_SSL_Verify_Server_Cert"),
		"Last_IO_Errno":                 c.Str("Last_IO_Errno"),
		"Last_IO_Error":                 c.Str("Last_IO_Error"),
		"Last_SQL_Errno":                c.Str("Last_SQL_Errno"),
		"Last_SQL_Error":                c.Str("Last_SQL_Error"),
		"Replicate_Ignore_Server_Ids":   c.Str("Replicate_Ignore_Server_Ids"),
		"Master_Server_Id":              c.Str("Master_Server_Id"),
		"Master_UUID":                   c.Str("Master_UUID"),
		"Master_Info_File":              c.Str("Master_Info_File"),
		"SQL_Delay":                     c.Str("SQL_Delay"),
		"SQL_Remaining_Delay":           c.Str("SQL_Remaining_Delay"),
		"Slave_SQL_Running_State":       c.Str("Slave_SQL_Running_State"),
		"Master_Retry_Count":            c.Str("Master_Retry_Count"),
		"Master_Bind":                   c.Str("Master_Bind"),
		"Last_IO_Error_Timestamp":       c.Str("Last_IO_Error_Timestamp"),
		"Last_SQL_Error_Timestamp":      c.Str("Last_SQL_Error_Timestamp"),
		"Master_SSL_Crl":                c.Str("Master_SSL_Crl"),
		"Master_SSL_Crlpath":            c.Str("Master_SSL_Crlpath"),
		"Retrieved_Gtid_Set":            c.Str("Retrieved_Gtid_Set"),
		"Executed_Gtid_Set":             c.Str("Executed_Gtid_Set"),
		"Auto_Position":                 c.Str("Auto_Position"),
		"Replicate_Rewrite_DB":          c.Str("Replicate_Rewrite_DB"),
		"Channel_Name":                  c.Str("Channel_Name"),
		"Master_TLS_Version":            c.Str("Master_TLS_Version"),
	}
)

// Map data to MapStr of server stats variables: http://dev.mysql.com/doc/refman/5.7/en/server-status-variables.html
// This is only a subset of the available values
func eventMapping(status map[string]string) common.MapStr {
	source := map[string]interface{}{}
	for key, val := range status {
		source[key] = val
	}
	return schema.Apply(source)
}

func rawEventMapping(status map[string]string) common.MapStr {
	source := common.MapStr{}
	for key, val := range status {
		// Only adds events which are not in the mapping
		if schema.HasKey(key) {
			continue
		}

		source[key] = val
	}
	return source
}
