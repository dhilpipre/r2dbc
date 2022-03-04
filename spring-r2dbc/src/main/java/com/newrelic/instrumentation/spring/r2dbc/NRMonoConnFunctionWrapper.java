package com.newrelic.instrumentation.spring.r2dbc;

import java.util.function.Function;

import com.newrelic.agent.bridge.AgentBridge;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;

import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;

public class NRMonoConnFunctionWrapper<T> implements Function<Connection, Mono<T>> {
	
	private Function<Connection, Mono<T>> delegate = null;
	private static boolean isTransformed = false;
	private Token token = null;
	
	public NRMonoConnFunctionWrapper(Function<Connection, Mono<T>> d, Token t) {
		if(!isTransformed) {
			AgentBridge.instrumentation.retransformUninstrumentedClass(getClass());
			isTransformed = true;
		}
		delegate = d;
		token = t;
	}

	@Override
	@Trace(async=true)
	public Mono<T> apply(Connection t) {
		if(token != null) {
			token.linkAndExpire();
			token = null;
		}
		
		return delegate != null ? delegate.apply(t) : Mono.empty();
	}

}
