package io.r2dbc.proxy.callback;

import java.lang.reflect.Method;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;

@Weave
public abstract class ResultCallbackHandler {

	@Trace
	public Object invoke(Object proxy, Method method, Object[] args) {
		if(method != null) {
			String methodName = method.getName();
			if(methodName != null && !methodName.isEmpty()) {
				NewRelic.getAgent().getTracedMethod().setMetricName("Custom","R2DBC","Proxy","ResultCallbackHandler","invoke",methodName);
			}
		}
		return Weaver.callOriginal();
	}
}
