package org.springframework.r2dbc.core;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.spring.r2dbc.NRFlux;
import com.newrelic.instrumentation.spring.r2dbc.NRMono;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Weave(type = MatchType.Interface)
public class RowsFetchSpec<T> {

	@Trace
	public Mono<T> one() {
		Token token = NewRelic.getAgent().getTransaction().getToken();
		if(token != null && !token.isActive()) {
			token = null;
		}
		Mono<T> result = Weaver.callOriginal();
		return new NRMono<T>(result, token);
	}

	@Trace
	public Mono<T> first() {
		Token token = NewRelic.getAgent().getTransaction().getToken();
		if(token != null && !token.isActive()) {
			token = null;
		}
		Mono<T> result = Weaver.callOriginal();
		return new NRMono<T>(result, token);
	}

	@Trace
	public Flux<T> all() {
		Token token = NewRelic.getAgent().getTransaction().getToken();
		if(token != null && !token.isActive()) {
			token = null;
		}
		Flux<T> result = Weaver.callOriginal();
		return new NRFlux<T>(result, token);
	}

}
