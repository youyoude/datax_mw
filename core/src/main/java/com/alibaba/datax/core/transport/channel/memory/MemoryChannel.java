package com.alibaba.datax.core.transport.channel.memory;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 内存Channel的具体实现，底层其实是一个ArrayBlockingQueue
 *
 */
public class MemoryChannel extends Channel {

	private static final Logger LOG = LoggerFactory.getLogger(Channel.class);

	private int bufferSize = 0;

	private AtomicInteger memoryBytes = new AtomicInteger(0);

//	private ReentrantLock lock;
	
//	private Condition notInsufficient, notEmpty;

	public MemoryChannel(final Configuration configuration,String channelKey) {
		super(configuration);
		ArrayBlockingQueue<Record> 	queue = new ArrayBlockingQueue<Record>(this.getCapacity());
		CoreConstant.QUEUECACHE.put(channelKey, queue);
		this.channelKey=channelKey;
		this.bufferSize = configuration.getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);

		ReentrantLock lock = new ReentrantLock();
		CoreConstant.LOCKCACHE.put(channelKey,lock);
//		notInsufficient = lock.newCondition();
		CoreConstant.CONDITIONnotInsufficientCACHE.put(channelKey,lock.newCondition()) ;
//		notEmpty = lock.newCondition();
		CoreConstant.CONDITIONnotEmptyCACHE.put(channelKey,lock.newCondition()) ;

	}

	@Override
	public void close() {
		super.close();
		try {
			CoreConstant.QUEUECACHE.get(channelKey).put(TerminateRecord.get());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void clear(){
		CoreConstant.QUEUECACHE.get(channelKey).clear();
	}

	@Override
	protected void doPush(Record r) {
		try {
//			LOG.info(" doPush {} remainingCapacity {}",this.channelKey,CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());
			long startTime = System.nanoTime();
			CoreConstant.QUEUECACHE.get(channelKey).put(r);
			waitWriterTime += System.nanoTime() - startTime;
            memoryBytes.addAndGet(r.getMemorySize());
//			LOG.info(" doPush {} remainingCapacity {}",this.channelKey,CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());

		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void doPushAll(Collection<Record> rs) {
		try {
//			LOG.info("doPushAll before {} remainingCapacity {}",this.channelKey,CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());

//			LOG.info(" doPushAll lock isLocked = {}", CoreConstant.LOCKCACHE.get(channelKey).isLocked());
			long startTime = System.nanoTime();
			CoreConstant.LOCKCACHE.get(channelKey).lockInterruptibly();
//			LOG.info(" doPushAll lock isLocked = {}", CoreConstant.LOCKCACHE.get(channelKey).isLocked());

			int bytes = getRecordBytes(rs);
			while (memoryBytes.get() + bytes > this.byteCapacity || rs.size() > CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity()) {
				CoreConstant.CONDITIONnotInsufficientCACHE.get(channelKey).await(200L, TimeUnit.MILLISECONDS);
//				LOG.info("doPushAll await {} {}",channelKey, CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());
            }
			CoreConstant.QUEUECACHE.get(channelKey).addAll(rs);
			waitWriterTime += System.nanoTime() - startTime;
			memoryBytes.addAndGet(bytes);
			CoreConstant.CONDITIONnotEmptyCACHE.get(channelKey).signalAll();
//			LOG.info("doPushAll after {} remainingCapacity {}",this.channelKey,CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());

		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			CoreConstant.LOCKCACHE.get(channelKey).unlock();
//			LOG.info(" doPushAll finally lock isLocked = {}", CoreConstant.LOCKCACHE.get(channelKey).isLocked());

		}
	}

	@Override
	protected Record doPull() {
		try {
//			LOG.info(" doPull {} remainingCapacity {}",this.channelKey,CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());

			long startTime = System.nanoTime();
			Record r = CoreConstant.QUEUECACHE.get(channelKey).take();

			waitReaderTime += System.nanoTime() - startTime;
			memoryBytes.addAndGet(-r.getMemorySize());
//			LOG.info("doPull {} remainingCapacity {}",this.channelKey,CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());

			return r;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	@Override
	protected void doPullAll(Collection<Record> rs) {
		assert rs != null;
		rs.clear();
		try {
//			LOG.info("doPullAll before {} remainingCapacity {}",this.channelKey,CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());
//			LOG.info(" doPullAll lock isLocked = {}", CoreConstant.LOCKCACHE.get(channelKey).isLocked());

			long startTime = System.nanoTime();
			CoreConstant.LOCKCACHE.get(channelKey).lockInterruptibly();
//			LOG.info(" doPullAll lock isLocked = {}", CoreConstant.LOCKCACHE.get(channelKey).isLocked());

			while (CoreConstant.QUEUECACHE.get(channelKey).drainTo(rs, bufferSize) <= 0) {
				CoreConstant.CONDITIONnotEmptyCACHE.get(channelKey).await(200L, TimeUnit.MILLISECONDS);

//				LOG.info("doPullAll await {}",CoreConstant.QUEUECACHE.get(channelKey).toArray().length);

//				Record r = CoreConstant.QUEUECACHE.get(channelKey).take();


//				LOG.info("doPullAll await {} {}",channelKey, CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());

			}
			waitReaderTime += System.nanoTime() - startTime;
			int bytes = getRecordBytes(rs);
			memoryBytes.addAndGet(-bytes);
			CoreConstant.CONDITIONnotInsufficientCACHE.get(channelKey).signalAll();
//			LOG.info("doPullAll after {} remainingCapacity {}",this.channelKey,CoreConstant.QUEUECACHE.get(channelKey).remainingCapacity());

		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			CoreConstant.LOCKCACHE.get(channelKey).unlock();
//			LOG.info(" doPullAll finally lock isLocked = {}", CoreConstant.LOCKCACHE.get(channelKey).isLocked());

		}
	}

	private int getRecordBytes(Collection<Record> rs){
		int bytes = 0;
		for(Record r : rs){
			bytes += r.getMemorySize();
		}
		return bytes;
	}

	@Override
	public int size() {
		return CoreConstant.QUEUECACHE.get(channelKey).size();
	}

	@Override
	public boolean isEmpty() {
		return CoreConstant.QUEUECACHE.get(channelKey).isEmpty();
	}

}
