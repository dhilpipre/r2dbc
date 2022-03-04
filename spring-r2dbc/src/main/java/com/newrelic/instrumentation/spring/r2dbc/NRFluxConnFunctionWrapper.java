package com.newrelic.instrumentation.spring.r2dbc;

import java.util.function.Function;

import com.newrelic.agent.bridge.AgentBridge;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;

import io.r2dbc.spi.Connection;
import reactor.core.publisher.Flux;

public class NRFluxConnFunctionWrapper<T> implements Function<Connection, Flux<T>> {
	
	private Function<Connection, Flux<T>> delegate = null;
	private static boolean isTransformed = false;
	private Token token = null;
	
	public NRFluxConnFunctionWrapper(Function<Connection, Flux<T>> d, Token t) {
		if(!isTransformed) {
			AgentBridge.instrumentation.retransformUninstrumentedClass(getClass());
			isTransformed = true;
		}
		delegate = d;
		token = t;
	}

	@Override
	@Trace(async=true)
	public Flux<T> apply(Connection t) {
		if(token != null) {
			token.linkAndExpire();
			token = null;
		}
		
		return delegate != null ? delegate.apply(t) : Flux.empty();
	}

}
