package com.newrelic.instrumentation.r2dbc.proxy;

import java.util.Map;

public class Utils {

	public static void addAttribute(Map<String, Object> attributes, String key, Object value) {
		if(attributes != null && key != null && !key.isEmpty() && value != null) {
			attributes.put(key, value);
		}
	}
	
	
}
