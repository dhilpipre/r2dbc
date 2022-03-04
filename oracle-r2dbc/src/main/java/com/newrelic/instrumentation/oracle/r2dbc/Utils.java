package com.newrelic.instrumentation.oracle.r2dbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import com.newrelic.agent.bridge.datastore.DatabaseVendor;
import com.newrelic.agent.database.SqlObfuscator;
import com.newrelic.agent.service.ServiceFactory;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.QueryConverter;

import io.r2dbc.spi.ConnectionFactoryOptions;

public class Utils {

	public static final String DATABASE_HOST = "Host";
	public static final String DATABASE_PORT = "Port";
	public static final String DATABASE_NAME = "Name";
	private static final String URL_START = "jdbc:oracle:thin:@";
	private static final HashMap<String, Map<String, Object>> databaseCache = new HashMap<String, Map<String, Object>>();
	
	public static void addAttribute(Map<String, Object> attributes, String key, Object value) {
		if(attributes != null && key != null && !key.isEmpty() && value != null) {
			attributes.put(key, value);
		}
	}
	
	public static String getAppName() {
		return ServiceFactory.getConfigService().getLocalAgentConfig().getApplicationName();
	}

	public static QueryConverter<String> getQueryConverter() {

		return new SqlQueryConverter(getAppName(), OracleDatabaseVendor.INSTANCE);
	}

	private static class SqlQueryConverter implements QueryConverter<String> {
		private final String appName;
		private final DatabaseVendor databaseVendor;

		public SqlQueryConverter(String appName, DatabaseVendor databaseVendor) {
			this.appName = appName;
			this.databaseVendor = databaseVendor;
		}

		@Override
		public String toRawQueryString(String rawQuery) {
			return rawQuery;
		}

		@Override
		public String toObfuscatedQueryString(String rawQuery) {
			SqlObfuscator sqlObfuscator = SqlObfuscator.getDefaultSqlObfuscator();
			NewRelic.getAgent().getLogger().log(Level.FINE, "\tRetrieved SqlObfuscator: {0}", sqlObfuscator);
			String dialect = databaseVendor.getType();
			NewRelic.getAgent().getLogger().log(Level.FINE, "\tUsing dialect: {0}", dialect);
			String result = sqlObfuscator.obfuscateSql(rawQuery, dialect);
			if(result == null) {
				result = rawQuery.replaceAll(":P\\d*\\w*", "?");
			} else {
				result = result.replaceAll(":P\\d*\\w*", "?");
			}
			NewRelic.getAgent().getLogger().log(Level.FINE, "\tReturning: {0}", result);
			return result;
		}
	}
	
	public static Map<String, Object> getConnectionInfo(Connection conn) {
		Map<String,Object> attributes = null;
		try {
			DatabaseMetaData metaData = conn.getMetaData();
			String url = metaData.getURL();
			if(url.toLowerCase().startsWith("r2dbc")) {
				attributes = getR2DBCAttributes(url);
			} else if(url.toLowerCase().startsWith("jdbc")) {
				attributes = getJDBCAttributes(url);
			}
		} catch (SQLException e) {
		}
		
		if(attributes == null) {
			attributes = new HashMap<String, Object>();
		}
		return attributes;
	}
	
	private static Map<String, Object> getR2DBCAttributes(String url) {
		
		Map<String, Object> attributes = databaseCache.get(url);

		if(attributes != null) return attributes;
		
		attributes = new HashMap<String, Object>();

		ConnectionFactoryOptions options = ConnectionFactoryOptions.parse(url);
		String host = options.getValue(ConnectionFactoryOptions.HOST);
		if(host != null && !host.isEmpty()) {
			attributes.put(DATABASE_HOST, host);
		}
		Integer port = options.getValue(ConnectionFactoryOptions.PORT);
		if(port != null) {
			attributes.put(DATABASE_PORT, port);
		}
		String database = options.getValue(ConnectionFactoryOptions.DATABASE);
		if(database != null && !database.isEmpty()) {
			attributes.put(DATABASE_NAME, database);
		}
		databaseCache.put(url, attributes);
		return attributes;
		
	}
	
	private static Map<String, Object> getJDBCAttributes(String url) {
		Map<String, Object> attributes = databaseCache.get(url);
		if(attributes != null)  return attributes;
		
		attributes = new HashMap<String, Object>();
		
		String host = null;
		Integer port = null;
		String dbName = null;
		if(url.startsWith(URL_START)) {
			String url2 = url.substring(URL_START.length());
			if(url2.startsWith("tcps:")) {
				url2 = url2.substring("tcps:".length());
			}
			int index = url2.indexOf(':');
			int index2 = url2.indexOf('/');
			if(index > -1) {
				host = url2.substring(0, index);
				if(index2 > -1) {
					port = Integer.parseInt(url2.substring(index+1, index2));
					dbName = url2.substring(index2+1);
				} else {
					port = Integer.parseInt(url2.substring(index+1));
				}
				
			} else {
				if(index2 > -1) {
					host = url2.substring(0, index2);
					dbName = url2.substring(index2+1);
				} else {
					host = url2;
				}
			}
			if(host != null && !host.isEmpty()) {
				attributes.put(DATABASE_HOST, host);
			}
			if(port != null) {
				attributes.put(DATABASE_PORT, port);
			}
			if(dbName != null && !dbName.isEmpty()) {
				attributes.put(DATABASE_NAME, dbName);
			}
			databaseCache.put(url, attributes);
		}
		return attributes;
	}
	
}
