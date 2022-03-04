package io.r2dbc.proxy.callback;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Token;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.NewField;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.WeaveAllConstructors;
import com.newrelic.api.agent.weaver.Weaver;

@Weave(type = MatchType.BaseClass) 
abstract class MethodInvocationSubscriber {
	
	@NewField
	public Token token = null;

	@WeaveAllConstructors
	public MethodInvocationSubscriber() {
		if(token == null) {
			token = NewRelic.getAgent().getTransaction().getToken();
			if(token != null && !token.isActive()) {
				token.expire();
				token = null;
			}
		}
	}
	
	public void cancel() {
		if(token != null) {
			token.expire();
			token = null;
		}
		Weaver.callOriginal();
	}
	
	@Trace(async = true)
	public void onComplete() {
		if(token != null) {
			token.linkAndExpire();
			token = null;
		}
		Weaver.callOriginal();
	}

	@Trace(async = true)
    public void onError(Throwable t) {
		NewRelic.noticeError(t);
		if(token != null) {
			token.linkAndExpire();
			token = null;
		}
		Weaver.callOriginal();
    }

	@Trace(async = true)
	public void onNext(Object object) {
		if(token != null) {
			token.link();
			token = null;
		}
		Weaver.callOriginal();
	}
	
}
