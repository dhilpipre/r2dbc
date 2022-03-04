package com.newrelic.instrumentation.spring.r2dbc;

import org.reactivestreams.Subscription;

import com.newrelic.agent.bridge.AgentBridge;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;

import reactor.core.CoreSubscriber;

public class NRSubscriber<T> implements CoreSubscriber<T> {
	
	private CoreSubscriber<T> delegate = null;
	private static boolean isTransformed = false;
	private Token token = null;
	
	public NRSubscriber(CoreSubscriber<T> d, Token t) {
		if(!isTransformed) {
			isTransformed = true;
			AgentBridge.instrumentation.retransformUninstrumentedClass(getClass());
		}
		delegate = d;
		token = t;
	}

	@Override
	@Trace(async = true)
	public void onSubscribe(Subscription s) {
		if(token != null) {
			token.link();
		} else {
			Token t = NewRelic.getAgent().getTransaction().getToken();
			if(t != null && t.isActive()) {
				token = t;
			} else if(t != null) {
				t.expire();
				t = null;
			}
		}
		delegate.onSubscribe(s);
	}

	@Override
	@Trace(async = true)
	public void onNext(T t) {
		if(token != null) {
			token.link();
		}
		delegate.onNext(t);
	}

	@Override
	@Trace(async = true)
	public void onError(Throwable t) {
		if(token != null) {
			token.linkAndExpire();
			token = null;
		}
		delegate.onError(t);
	}

	@Override
	@Trace(async = true)
	public void onComplete() {
		if(token != null) {
			token.linkAndExpire();
			token = null;
		}
		delegate.onComplete();
	}

}
