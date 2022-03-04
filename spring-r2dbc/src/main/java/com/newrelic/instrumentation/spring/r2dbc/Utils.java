package com.newrelic.instrumentation.spring.r2dbc;

import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.newrelic.api.agent.Token;

import reactor.core.publisher.Operators;

public class Utils {

	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> Function<? super Publisher<T>, ? extends Publisher<T>> wrapSubscriber(Token token) {
		return Operators.liftPublisher((publisher,subscriber) -> new NRSubscriber(subscriber, token));
	}
}
