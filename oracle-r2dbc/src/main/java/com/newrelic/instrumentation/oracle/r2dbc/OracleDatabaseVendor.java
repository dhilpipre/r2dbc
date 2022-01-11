package com.newrelic.instrumentation.oracle.r2dbc;

import java.sql.SQLException;

import com.newrelic.agent.bridge.datastore.DatabaseVendor;
import com.newrelic.agent.bridge.datastore.DatastoreVendor;
import com.newrelic.agent.bridge.datastore.JdbcDatabaseVendor;

public class OracleDatabaseVendor extends JdbcDatabaseVendor {
	
	public static final DatabaseVendor INSTANCE = new OracleDatabaseVendor();

	private OracleDatabaseVendor() {
		super("Oracle", "oracle", false);
	}

	@Override
	public DatastoreVendor getDatastoreVendor() {
		return DatastoreVendor.Oracle;
	}

	@Override
    public String getExplainPlanSql(String sql) throws SQLException {
        return "EXPLAIN PLAN FOR " + sql;
    }
}
