package com.github.dexecutor.hazelcast;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.hazelcast.core.QueueStore;

public class DiskQueueStore<T, R> implements QueueStore<Object> {

	@Override
	public void store(Long key, Object value) {
		Map<Long, Object> map = read("store");
		
		map.put(key, value);
		write(map);
	}

	@Override
	public void storeAll(Map<Long, Object> map) {
		Map<Long, Object> result = read("storeAll");
		Iterator<Map.Entry<Long, Object>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Long, Object> pair = it.next();
			result.put(pair.getKey(), pair.getValue());
		}
		write(result);
	}

	@Override
	public void delete(Long key) {
		Map<Long, Object> map = read("delete");
		map.remove(key);
		write(map);
	}

	@Override
	public void deleteAll(Collection<Long> keys) {
		Map<Long, Object> result = read("deleteAll");
		for (Long key : keys) {
			result.remove(key);
		}
		
		write(result);

	}

	@Override
	public Object load(Long key) {
		Map<Long, Object> result = read("load");
		return result.get(key);
	}

	@Override
	public Map<Long, Object> loadAll(Collection<Long> keys) {
		Map<Long, Object> map = read("loadAll");
		Map<Long, Object> result = new HashMap<>();
		for (Long key : keys) {
			result.put(key, map.get(key));
		}
		return result;
	}

	@Override
	public Set<Long> loadAllKeys() {
		Map<Long, Object> map = read("loadAllKeys");		
		return map.keySet();
	}

	@SuppressWarnings("unchecked")
	private Map<Long, Object> read(String method) {
		Map<Long, Object> result = null;

		try {
			FileInputStream fis = new FileInputStream("map.ser");
			ObjectInputStream ois = new ObjectInputStream(fis);
			result = (Map<Long, Object>) ois.readObject();
			ois.close();
		} catch (FileNotFoundException e) {
			result = new HashMap<>();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Iterator<Map.Entry<Long, Object>> it = result.entrySet().iterator();
		System.out.println("**** content *** " +  method);
		while (it.hasNext()) {
			Map.Entry<Long, Object> pair = it.next();
			System.out.println(pair.getValue());
		}
		return result;
	}

	private void write(Map<Long, Object> map) {
		
		try {
			FileOutputStream fos = new FileOutputStream("map.ser");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(map);
			oos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
