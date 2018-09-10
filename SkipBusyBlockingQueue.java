package com.util.concurrent.skipBusyBlockingQueue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SkipBusyBlockingQueue<E extends SkipBusyBlockingQueueElement<?>,T>{

	private final Queue<T> queue = new LinkedList<T>();
	private final Lock lock=new ReentrantLock();
	private final Condition lockCondition = lock.newCondition();
	private final Map<T,EventSequenceQueue> dataMap =new HashMap<T,EventSequenceQueue>();
	
	public void put(E element){
		lock.lock();
		try{			
			T uniqueId = (T) element.getUniqueId();
			if(dataMap.containsKey(uniqueId)){
				dataMap.get(uniqueId).putElement(element);
			}else{
				EventSequenceQueue sequencedQueue = new EventSequenceQueue<E,T>(uniqueId);
				sequencedQueue.putElement(element);
				dataMap.put(uniqueId, sequencedQueue);
				queue.add(uniqueId);
			}
			
		}finally{
			lockCondition.signalAll();
			lock.unlock();
		}
	}
	
	public E take() throws InterruptedException{
		lock.lock();
		try{
		while(queue.isEmpty()){
			lockCondition.await();
		}
		for(T uniqueID : queue){
			E element = (E) dataMap.get(uniqueID).takeElement();
			if(element!=null)
				return element;
		}
		lockCondition.await();
		}finally{
			lock.unlock();
		}
		return take();		
	}
	
	public void releaseLock(T uniqueId){
		lock.lock();
		try{
			if(dataMap.containsKey(uniqueId)){
				EventSequenceQueue eventSequenceQueue = dataMap.get(uniqueId);
				eventSequenceQueue.releaseExternalLock();
				if(eventSequenceQueue.isEmpty()){
				dataMap.remove(uniqueId);
				queue.remove(uniqueId);
				}
			}
		}finally{
			lockCondition.signalAll();
			lock.unlock();
		}
	}
	
	private class EventSequenceQueue<E,T>{
		private final Queue<E> queue = new LinkedList<E>();
		private final Lock extrenallock=new ReentrantLock();
		private final Semaphore semaphore = new Semaphore(1);
		private final T queueId;
		
		EventSequenceQueue(T queueId){
			this.queueId=queueId;
		}
		
		T getQueueId(){
		return 	queueId;
		}
		
		E takeElement(){
			if(extrenallock.tryLock()){
			if(semaphore.tryAcquire())
			return queue.poll();			
			}else{
				extrenallock.unlock();
			}
			return null;			
		}
		
		void releaseExternalLock(){
			semaphore.release();
			extrenallock.unlock();
		}
		
		boolean putElement(E element){
			return queue.add(element);						
		}
		
		boolean isEmpty(){
			if(extrenallock.tryLock()){
				return queue.isEmpty();
			}else
				extrenallock.unlock();
			return false;
		}
		
		int size(){
			return queue.size();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;			
			result = prime * result
					+ ((queueId == null) ? 0 : queueId.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			EventSequenceQueue other = (EventSequenceQueue) obj;			
			if (queueId == null) {
				if (other.queueId != null)
					return false;
			} else if (!queueId.equals(other.queueId))
				return false;
			return true;
		}
		
	}
}
