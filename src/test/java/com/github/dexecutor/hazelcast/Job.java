package com.github.dexecutor.hazelcast;

import com.github.dexecutor.core.DefaultDependentTasksExecutor;
import com.github.dexecutor.core.DependentTasksExecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskProvider;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;

public class Job {

	public void run(boolean isMaster, final String nodeName) throws Exception {

		Config cfg = new Config();
		HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
		IExecutorService executorService = instance.getExecutorService("test");

		if (isMaster) {
			DefaultDependentTasksExecutor<Integer, Integer> dexecutor = newTaskExecutor(executorService);

			buildGraph(dexecutor);
			dexecutor.execute(ExecutionConfig.TERMINATING);
		}

		System.out.println("Ctrl+D/Ctrl+Z to stop.");
	}
	
	private DefaultDependentTasksExecutor<Integer, Integer> newTaskExecutor(IExecutorService executorService) {
		DependentTasksExecutorConfig<Integer, Integer> config = new DependentTasksExecutorConfig<Integer, Integer>(
				new HazelcastExecutionEngine<Integer, Integer>(executorService), new SleepyTaskProvider());
		return new DefaultDependentTasksExecutor<Integer, Integer>(config);
	}

	private static class SleepyTaskProvider implements TaskProvider<Integer, Integer> {

		public Task<Integer, Integer> provideTask(final Integer id) {
			return new HazelcastTask(id);
		}
	}

	private void buildGraph(final DefaultDependentTasksExecutor<Integer, Integer> dexecutor) {
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
