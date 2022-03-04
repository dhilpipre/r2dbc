package com.newrelic.instrumentation.spring.r2dbc;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Token;

import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;

public class NRMono<T> extends Mono<T> {
	
	private Mono<T> delegate = null;
	private Token token = null;
	
	public NRMono(Mono<T> d, Token t) {
		delegate = d;
		token = t;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Token t = null;
		if(token != null) {
			t = token;
			token = null;
		} else {
			t = NewRelic.getAgent().getTransaction().getToken();
			if(t != null && !t.isActive()) {
				t.expire();
				t = null;
			}
		}
		@SuppressWarnings({ "rawtypes", "unchecked" })
		NRSubscriber<? super T> wrapper = new NRSubscriber(actual,t);
		
		delegate.subscribe(wrapper);
	}

}
