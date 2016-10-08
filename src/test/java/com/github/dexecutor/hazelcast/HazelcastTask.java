package com.github.dexecutor.hazelcast;

import com.github.dexecutor.core.task.Task;

public class HazelcastTask extends Task<Integer, Integer> {

	private static final long serialVersionUID = 1L;

	public HazelcastTask(final Integer id) {
		setId(id);
	}

	@Override
	public Integer execute() {
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return getId();
	}
}
