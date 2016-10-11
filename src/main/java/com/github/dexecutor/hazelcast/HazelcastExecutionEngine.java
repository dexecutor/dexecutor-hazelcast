/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
/**
 * Distributed Execution Engine using Hazelcast
 * 
 * @author Nadeem Mohammad
 *
 * @param <T> Type of Node/Task ID
 * @param <R> Type of Node/Task result
 */
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
