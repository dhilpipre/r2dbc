package org.springframework.r2dbc.core;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.spring.r2dbc.NRMono;

import reactor.core.publisher.Mono;

@Weave(type=MatchType.Interface)
public abstract class UpdatedRowsFetchSpec {

	@Trace
	public Mono<Integer> rowsUpdated() {
		Token token = NewRelic.getAgent().getTransaction().getToken();
		if(token != null && !token.isActive()) {
			token = null;
		}
		Mono<Integer> result = Weaver.callOriginal();
		return new NRMono<Integer>(result, token);
	}
}
