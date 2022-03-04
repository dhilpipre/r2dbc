package oracle.r2dbc.impl;

import java.util.HashMap;
import java.util.Map;

import org.reactivestreams.Publisher;

import com.newrelic.agent.database.DefaultDatabaseStatementParser;
import com.newrelic.agent.database.ParsedDatabaseStatement;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.NewField;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.WeaveAllConstructors;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.oracle.r2dbc.NROnCompleteRunnable;
import com.newrelic.instrumentation.oracle.r2dbc.NRSuccessConsumer;
import com.newrelic.instrumentation.oracle.r2dbc.OracleDatabaseVendor;
import com.newrelic.instrumentation.oracle.r2dbc.Utils;

import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Weave
abstract class OracleStatementImpl {
	
	private final String sql = Weaver.callOriginal();
	private final java.sql.Connection jdbcConnection = Weaver.callOriginal();
	@NewField
	private Map<String, Object> dbAttributes = null;
	

	@WeaveAllConstructors
	public OracleStatementImpl() {
		if(jdbcConnection != null &&  dbAttributes == null) {
			dbAttributes = Utils.getConnectionInfo(jdbcConnection);
			
		}
	}
	
	@Trace
	public Publisher<? extends Result> execute() {
		DefaultDatabaseStatementParser parser = new DefaultDatabaseStatementParser();
		ParsedDatabaseStatement parsed = parser.getParsedDatabaseStatement(OracleDatabaseVendor.INSTANCE, sql, null);
		if(dbAttributes == null) {
			dbAttributes = new HashMap<String, Object>();
		}
		Publisher<? extends Result> pub = Weaver.callOriginal();
		if(pub instanceof Mono) {
			Mono <? extends Result> mono = (Mono<? extends Result>)pub;
			return mono.doOnSuccess(new NRSuccessConsumer<>(sql, parsed.getOperation(), parsed.getModel(), dbAttributes));
		}
		if(pub instanceof Flux) {
			Flux<? extends Result> flux = (Flux<? extends Result>)pub;
			return flux.doOnComplete(new NROnCompleteRunnable(sql, parsed.getOperation(), parsed.getModel(), dbAttributes));
		}
		return pub;
	}
}
