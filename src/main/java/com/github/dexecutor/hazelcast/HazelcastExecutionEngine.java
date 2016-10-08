package com.github.dexecutor.hazelcast;

import static com.github.dexecutor.core.support.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dexecutor.core.ExecutionEngine;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskExecutionException;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;

public class HazelcastExecutionEngine<T extends Comparable<T>, R> implements ExecutionEngine<T, R> {

	private static final Logger logger = LoggerFactory.getLogger(HazelcastExecutionEngine.class);

	private Collection<T> erroredTasks = new CopyOnWriteArraySet<T>();

	private final IExecutorService executorService;
	private BlockingQueue<Future<ExecutionResult<T,R>>> completionQueue;

	public HazelcastExecutionEngine(final IExecutorService executorService) {
		this(executorService, new LinkedBlockingQueue<Future<ExecutionResult<T,R>>>());
	}

	public HazelcastExecutionEngine(final IExecutorService executorService, final BlockingQueue<Future<ExecutionResult<T,R>>> completionQueue) {
		checkNotNull(executorService, "Executor Service should not be null");
		checkNotNull(completionQueue, "Blocking Queue should not be null");
		this.executorService = executorService;
		this.completionQueue = completionQueue;
	}

	@Override
	public void submit(final Task<T, R> task) {
		logger.debug("Received Task {}",  task.getId());
		executorService.submit(new SerializableCallable<T, R>(task), new ExecutionCallback<ExecutionResult<T, R>>() {

			@Override
			public void onResponse(ExecutionResult<T, R> response) {
				completionQueue.add(new ValueFuture<ExecutionResult<T,R>>((ExecutionResult<T, R>) response));				
			}

			@Override
			public void onFailure(Throwable t) {
				logger.error("Error Executing Task {}", task.getId());				
			}
		});
	}

	@Override
	public ExecutionResult<T, R> processResult() throws TaskExecutionException {
		ExecutionResult<T, R> executionResult;
		try {
			executionResult = completionQueue.take().get();
			if (executionResult.isSuccess()) {				
				erroredTasks.remove(executionResult.getId());
			} else {
				erroredTasks.add(executionResult.getId());
			}
			return executionResult;
		} catch (InterruptedException | ExecutionException e) {
			throw new TaskExecutionException("Task interrupted");
		}
	}

	@Override
	public boolean isDistributed() {
		return true;
	}

	@Override
	public boolean isAnyTaskInError() {
		return !this.erroredTasks.isEmpty();
	}
}
