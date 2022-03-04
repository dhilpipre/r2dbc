package org.springframework.r2dbc.connection;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;

import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;

import reactor.core.publisher.Mono;

@Weave
public abstract class R2dbcTransactionManager {

	@Trace
	protected Mono<Void> doBegin(TransactionSynchronizationManager synchronizationManager, Object transaction,
			TransactionDefinition definition) {
		return Weaver.callOriginal();
	}
	
	@Trace
	protected Mono<Void> doCommit(TransactionSynchronizationManager TransactionSynchronizationManager,
			GenericReactiveTransaction status)  {
		return Weaver.callOriginal();
	}
	
	@Trace
	protected Mono<Void> doResume(TransactionSynchronizationManager synchronizationManager,
			Object transaction, Object suspendedResources) {
		return Weaver.callOriginal();
	}
	
	@Trace
	protected Mono<Void> doRollback(TransactionSynchronizationManager TransactionSynchronizationManager,
			GenericReactiveTransaction status)  {
		return Weaver.callOriginal();
	}
	
	@Trace
	protected Mono<Object> doSuspend(TransactionSynchronizationManager synchronizationManager, Object transaction) {
		return Weaver.callOriginal();
	}
}
