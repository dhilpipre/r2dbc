package org.springframework.r2dbc.core;

import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.r2dbc.core.DatabaseClient.GenericExecuteSpec;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.spring.r2dbc.NRFluxConnFunctionWrapper;
import com.newrelic.instrumentation.spring.r2dbc.NRMonoConnFunctionWrapper;

import io.r2dbc.spi.Connection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Weave
class DefaultDatabaseClient {

	@Trace
	public GenericExecuteSpec sql(Supplier<String> sqlSupplier)  {
		return Weaver.callOriginal();
	}
	
	public <T> Mono<T> inConnection(Function<Connection, Mono<T>> action) {
		if(!(action instanceof NRMonoConnFunctionWrapper)) {
			Token t = NewRelic.getAgent().getTransaction().getToken();
			if(t != null && t.isActive()) {
				NRMonoConnFunctionWrapper<T> wrapper = new NRMonoConnFunctionWrapper<T>(action, t);
				action = wrapper;
			} else if(t != null) {
				t.expire();
				t = null;
			}
			
		}
		return Weaver.callOriginal();
	}
	
	public <T> Flux<T> inConnectionMany(Function<Connection, Flux<T>> action) {
		if(!(action instanceof NRMonoConnFunctionWrapper)) {
			Token t = NewRelic.getAgent().getTransaction().getToken();
			if(t != null && t.isActive()) {
				NRFluxConnFunctionWrapper<T> wrapper = new NRFluxConnFunctionWrapper<T>(action, t);
				action = wrapper;
			} else if(t != null) {
				t.expire();
				t = null;
			}
			
		}
		return Weaver.callOriginal();
	}
}
