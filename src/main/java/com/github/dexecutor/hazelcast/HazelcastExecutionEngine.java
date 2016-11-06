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

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dexecutor.core.DexecutorState;
import com.github.dexecutor.core.ExecutionEngine;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskExecutionException;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
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

	private final DexecutorState<T, R> dexecutorState;

	private final IExecutorService executorService;
	/**
	 * http://docs.hazelcast.org/docs/3.5/manual/html/queueconfiguration.html
	 * http://docs.hazelcast.org/docs/3.5/manual/html/queue-persistence.html
	 */
	private BlockingQueue<ExecutionResult<T,R>> completionQueue;

	public HazelcastExecutionEngine(final DexecutorState<T, R> dexecutorState, final HazelcastInstance instance, final String cacheName) {
		checkNotNull(instance, "HazelcastInstance should not be null");
		checkNotNull(cacheName, "cache nname should not be null");
		this.dexecutorState = dexecutorState;
		this.executorService = instance.getExecutorService(cacheName + "-ES");
		this.completionQueue = instance.getQueue(cacheName + "-BQ");
	}

	@Override
	public void submit(final Task<T, R> task) {
		logger.debug("Received Task {}",  task.getId());
		executorService.submit(new SerializableCallable<T, R>(task), new ExecutionCallback<ExecutionResult<T, R>>() {

			@Override
			public void onResponse(ExecutionResult<T, R> response) {
				completionQueue.add(response);				
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
			executionResult = completionQueue.take();
			if (executionResult.isSuccess()) {
				this.dexecutorState.removeErrored(executionResult.getId());
			} else {
				this.dexecutorState.addErrored(executionResult.getId());
			}
			return executionResult;
		} catch (InterruptedException e) {
			throw new TaskExecutionException("Task interrupted");
		}
	}

	@Override
	public boolean isDistributed() {
		return true;
	}

	@Override
	public boolean isAnyTaskInError() {
		return this.dexecutorState.erroredCount() > 0;
	}
}
