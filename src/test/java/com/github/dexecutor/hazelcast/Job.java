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

import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskProvider;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Job {

	private static final String CACHE_NAME = "test";

	public void run(boolean isMaster, final String nodeName) throws Exception {

		Config cfg = new Config();
		/*QueueConfig queueConfig = new QueueConfig(CACHE_NAME);


		queueConfig.setName( CACHE_NAME + "-BQ").setBackupCount(1)
		        .setMaxSize(0).setStatisticsEnabled( true);

		QueueStoreConfig sc = new QueueStoreConfig(); 
		
		sc.setEnabled (true)
		        //.setClassName(DiskQueueStore.class.getName())
		        .setStoreImplementation(new DiskQueueStore<Integer, Integer>())
		        .setProperty( "binary", "false" );
		
		queueConfig.setQueueStoreConfig(sc);
		
		cfg.addQueueConfig(queueConfig);*/

		HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);

		if (isMaster) {
			DefaultDexecutor<Integer, Integer> dexecutor = newTaskExecutor(instance);

			buildGraph(dexecutor);
			dexecutor.execute(ExecutionConfig.TERMINATING);
			//dexecutor.recoverExecution(ExecutionConfig.TERMINATING);
		}

		System.out.println("Ctrl+D/Ctrl+Z to stop.");
	}

	private DefaultDexecutor<Integer, Integer> newTaskExecutor(HazelcastInstance instance) {
		DexecutorConfig<Integer, Integer> config = new DexecutorConfig<Integer, Integer>(
				new HazelcastExecutionEngine<Integer, Integer>(instance, CACHE_NAME), new SleepyTaskProvider());
		config.setDexecutorState(new HazelcastDexecutorState<Integer, Integer>(CACHE_NAME, instance));
		return new DefaultDexecutor<Integer, Integer>(config);
	}

	private static class SleepyTaskProvider implements TaskProvider<Integer, Integer> {

		public Task<Integer, Integer> provideTask(final Integer id) {
			return new HazelcastTask(id);
		}
	}

	private void buildGraph(final DefaultDexecutor<Integer, Integer> dexecutor) {
		dexecutor.addDependency(1, 2);
		dexecutor.addDependency(1, 2);
		dexecutor.addDependency(1, 3);
		dexecutor.addDependency(3, 4);
		dexecutor.addDependency(3, 5);
		dexecutor.addDependency(3, 6);
		dexecutor.addDependency(2, 7);
		dexecutor.addDependency(2, 9);
		dexecutor.addDependency(2, 8);
		dexecutor.addDependency(9, 10);
		dexecutor.addDependency(12, 13);
		dexecutor.addDependency(13, 4);
		dexecutor.addDependency(13, 14);
		dexecutor.addIndependent(11);
	}	
}
