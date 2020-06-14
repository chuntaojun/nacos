/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.Objects;
import com.alibaba.nacos.naming.misc.Loggers;
import io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;

/**
 * Refer to the data expiration algorithm implemented by Netty time wheel
 * {@link io.grpc.netty.shaded.io.netty.util.HashedWheelTimer}
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@SuppressWarnings("all")
public class Lessor {

	private static final AtomicIntegerFieldUpdater<Lessor> WORKER_STATE_UPDATER =
			AtomicIntegerFieldUpdater.newUpdater(Lessor.class, "workerState");

	private final Thread workerThread;
	private final Worker worker = new Worker();

	public static final int WORKER_STATE_INIT = 0;
	public static final int WORKER_STATE_STARTED = 1;
	public static final int WORKER_STATE_SHUTDOWN = 2;
	@SuppressWarnings({ "unused", "FieldMayBeFinal" })
	private volatile int workerState; // 0 - init, 1 - started, 2 - shut down

	private final long tickDuration;
	private final Slot[] wheel;
	private final int mask;
	private final CountDownLatch startTimeInitialized = new CountDownLatch(1);
	private final Queue<OverdueWrapper> timeouts = PlatformDependent.newMpscQueue();
	private final BiConsumer<Collection<Instance>, Boolean> consumer;
	private final Set<Instance> alreadyExpire = new HashSet<>();
	private final Set<Instance> unHealth = new HashSet<>();

	private volatile long startTime;

	public static Lessor createLessor(final int slotNum, final Duration duration, final BiConsumer<Collection<Instance>, Boolean> consumer) {
		return new Lessor(slotNum, duration, consumer);
	}

	private Lessor(final int slotNum, final Duration duration, final BiConsumer<Collection<Instance>, Boolean> consumer) {
		this.tickDuration = duration.toNanos();
		this.wheel = createWheel(ConvertUtils.normalizeToPower2(slotNum));
		this.mask = wheel.length - 1;
		ThreadFactory threadFactory = new NameThreadFactory(
				"nacos.core.item.expire.wheel");
		this.workerThread = threadFactory.newThread(worker);
		this.consumer = consumer;
	}

	private Slot[] createWheel(final int slotNum) {
		Slot[] wheel = new Lessor.Slot[slotNum];
		for (int i = 0; i < slotNum; i ++) {
			wheel[i] = new Slot();
		}
		return wheel;
	}

	/**
	 * Starts the background thread explicitly.  The background thread will
	 * start automatically on demand even if you did not call this method.
	 *
	 * @throws IllegalStateException if this timer has been
	 *                               {@linkplain #stop() stopped} already
	 */
	public void start() {
		switch (WORKER_STATE_UPDATER.get(this)) {
		case WORKER_STATE_INIT:
			if (WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
				workerThread.start();
			}
			break;
		case WORKER_STATE_STARTED:
			break;
		case WORKER_STATE_SHUTDOWN:
			throw new IllegalStateException("cannot be started once stopped");
		default:
			throw new Error("Invalid WorkerState");
		}

		// Wait until the startTime is initialized by the worker.
		while (startTime == 0) {
			try {
				startTimeInitialized.await();
			} catch (InterruptedException ignore) {
				// Ignore - it will be ready very soon.
			}
		}
	}

	public void stop() {
		if (Thread.currentThread() == workerThread) {
			throw new IllegalStateException(
					Lessor.class.getSimpleName() +
							".stop() cannot be called from " +
							Lessor.class.getSimpleName());
		}

		if (!WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
			// workerState can be 0 or 2 at this moment - let it always be 2.
			if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
				// TODO
			}
			return;
		}

		while (workerThread.isAlive()) {
			workerThread.interrupt();
			try {
				workerThread.join(100);
			} catch (InterruptedException ignored) {
				Thread.currentThread().interrupt();
			}
		}
		timeouts.clear();
		unHealth.clear();
		alreadyExpire.clear();
	}

	public void addItem(Instance overdue, long delayNs) {
		start();

		Objects.requireNonNull(overdue, "overdue");
		Objects.requireTrue(delayNs > 0, "delayNs must a positive number : " + delayNs);

		// Add the timeout to the timeout queue which will be processed on the next tick.
		// During processing all the queued HashedWheelTimeouts will be added to the correct HashedWheelBucket.
		long deadline = System.nanoTime() + delayNs - startTime;

		// Guard against overflow.
		if (deadline < 0) {
			deadline = Long.MAX_VALUE;
		}
		OverdueWrapper wrapper = new OverdueWrapper(overdue, deadline);
		timeouts.add(wrapper);
	}

	private class Worker implements Runnable {

		private long tick;

		@Override
		public void run() {
			// Initialize the startTime.
			startTime = System.nanoTime();
			if (startTime == 0) {
				// We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
				startTime = 1;
			}

			// Notify the other threads waiting for the initialization at start().
			startTimeInitialized.countDown();

			do {
				final long deadline = waitForNextTick();

				unHealth.clear();
				alreadyExpire.clear();

				if (deadline > 0) {
					int idx = (int) (tick & mask);
					Slot bucket = wheel[idx];
					transferTimeoutsToBuckets();
					bucket.expireTimeouts();
					if (CollectionUtils.isNotEmpty(unHealth)) {
						consumer.accept(unHealth, true);
					}
					if (CollectionUtils.isNotEmpty(alreadyExpire)) {
						consumer.accept(alreadyExpire, false);
					}
					tick++;
				}
			} while (WORKER_STATE_UPDATER.get(Lessor.this) == WORKER_STATE_STARTED);
		}

		private void transferTimeoutsToBuckets() {
			// transfer only max. 100000 overdue per tick to prevent a thread to stale the workerThread when it just
			// adds new timeouts in a loop.
			for (int i = 0; i < 100_000L; i++) {
				OverdueWrapper timeout = timeouts.poll();
				if (timeout == null) {
					// all processed
					break;
				}

				if (timeout.isCancelled) {
					continue;
				}

				long calculated = timeout.deadline / tickDuration;
				timeout.remainingRounds = (calculated - tick) / wheel.length;

				final long ticks = Math.max(calculated, tick); // Ensure we don't schedule for past.
				int stopIndex = (int) (ticks & mask);

				Slot bucket = wheel[stopIndex];
				bucket.addTimeout(timeout);
			}
		}

		/**
		 * calculate goal nanoTime from startTime and current tick number,
		 * then wait until that goal has been reached.
		 * @return Long.MIN_VALUE if received a shutdown request,
		 * current time otherwise (with Long.MIN_VALUE changed by +1)
		 */
		private long waitForNextTick() {
			long deadline = tickDuration * (tick + 1);

			for (; ; ) {
				final long currentTime = System.nanoTime() - startTime;
				long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

				if (sleepTimeMs <= 0) {
					if (currentTime == Long.MIN_VALUE) {
						return -Long.MAX_VALUE;
					}
					else {
						return currentTime;
					}
				}

				// Check if we run on windows, as if thats the case we will need
				// to round the sleepTime as workaround for a bug that only affect
				// the JVM if it runs on windows.
				//
				// See https://github.com/netty/netty/issues/356
				if (PlatformDependent.isWindows()) {
					sleepTimeMs = sleepTimeMs / 10 * 10;
				}

				try {
					Thread.sleep(sleepTimeMs);
				}
				catch (InterruptedException ignored) {
					if (WORKER_STATE_UPDATER.get(Lessor.this) == WORKER_STATE_SHUTDOWN) {
						return Long.MIN_VALUE;
					}
				}
			}
		}

		public long getTick() {
			return tick;
		}
	}

	private class OverdueWrapper {

		long remainingRounds;
		private final long deadline;

		private OverdueWrapper prev;
		private OverdueWrapper next;
		private final Instance target;
		boolean isCancelled = false;

		/** The bucket to which the timeout was added */
		private Slot slot;

		public OverdueWrapper(Instance target, final long deadline) {
			this.target = target;
			this.deadline = deadline;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			OverdueWrapper that = (OverdueWrapper) o;
			return Objects.equals(target, that.target);
		}

		@Override
		public int hashCode() {
			return Objects.hash(target);
		}

		public void setCancelled(boolean cancelled) {
			isCancelled = cancelled;
		}
	}

	/** May not be pushed back into the wheel of time */
	private static final short NOT_PUSH_BACK = -1;

	/** need to push back the time wheel to wait for it to be removed due to the examination as an unhealthy state */
	private static final short PUSH_BACK_UN_HEALTH = 1;

	/** Because the removed instance is marked, it cannot be deleted and pushed back to the time wheel */
	private static final short PUSH_BACK_NEED_DELETE = 2;

	/** The example heartbeat is processed normally, and the time wheel is calculated again */
	private static final short PUSH_BACK_NORMAL = 3;

	private class Slot {
		// Used for the linked-list datastructure
		private OverdueWrapper head;
		private OverdueWrapper tail;

		/**
		 * Add {@link OverdueWrapper} to this bucket.
		 */
		public void addTimeout(OverdueWrapper timeout) {
			assert timeout.slot == null;
			timeout.slot = this;
			if (head == null) {
				head = tail = timeout;
			} else {
				tail.next = timeout;
				timeout.prev = tail;
				tail = timeout;
			}
		}

		/**
		 * Expire all {@link OverdueWrapper}s for the given {@code deadline}.
		 */
		public void expireTimeouts() {
			OverdueWrapper timeout = head;

			long currentTime = System.currentTimeMillis();

			// process all timeouts
			while (timeout != null) {
				OverdueWrapper next = timeout.next;
				if (timeout.remainingRounds <= 0) {
					final Instance instance = timeout.target;

					next = remove(timeout);

					long delayNs = -1L;
					short pullBack = NOT_PUSH_BACK;
					boolean isHealth = true;

					// 当前实例健康，但是心跳超时，先行设置实例为不健康
					if (instance.getLastBeat() + instance.getInstanceHeartBeatTimeOut() - currentTime < 0) {
						isHealth = false;
						if (!instance.isMarked() && instance.isHealthy()) {
							instance.setHealthy(false);
							unHealth.add(instance);
						}

						// 假设当前实例虽然不健康，但是还不至于需要被摘除，因此需要被重新压回到时间轮中进行摘除计算。
						pullBack = PUSH_BACK_UN_HEALTH;
						delayNs = (instance.getLastBeat() + instance.getIpDeleteTimeout() - currentTime) * 1000000L;
					}

					// 当前实例不健康，且已经达到了需要被摘除的时间点
					if (instance.getLastBeat() + instance.getIpDeleteTimeout() - currentTime <= 0) {
						isHealth = false;
						if (!instance.isMarked()) {
							// 实例被摘除，无需重新压入时间轮
							alreadyExpire.add(instance);
							pullBack = NOT_PUSH_BACK;
							delayNs = -1L;
						} else {
							// 实例不能被摘除，重新压入时间轮
							pullBack = PUSH_BACK_NEED_DELETE;
							delayNs = instance.getIpDeleteTimeout();
						}
					}

					// 心跳没有超时，也没有需要被摘除，即实例的租约续上了，重新压入
					if (isHealth) {
						pullBack = PUSH_BACK_NORMAL;
						delayNs = (instance.getLastBeat() + instance.getInstanceHeartBeatTimeOut() - currentTime) * 1000000L;
					}

					if (pullBack != NOT_PUSH_BACK) {
						Loggers.EVT_LOG.debug("Instance lease recalculation : {}, less delay : {}, pull-back level : {}", instance, delayNs, pullBack);
						addItem(instance, delayNs);
					}
				} else {
					timeout.remainingRounds --;
				}
				timeout = next;
			}
		}

		public OverdueWrapper remove(OverdueWrapper timeout) {
			OverdueWrapper next = timeout.next;
			// remove timeout that was either processed or cancelled by updating the linked-list
			if (timeout.prev != null) {
				timeout.prev.next = next;
			}
			if (timeout.next != null) {
				timeout.next.prev = timeout.prev;
			}

			if (timeout == head) {
				// if timeout is also the tail we need to adjust the entry too
				if (timeout == tail) {
					tail = null;
					head = null;
				} else {
					head = next;
				}
			} else if (timeout == tail) {
				// if the timeout is the tail modify the tail to be the prev node.
				tail = timeout.prev;
			}
			// null out prev, next and bucket to allow for GC.
			timeout.prev = null;
			timeout.next = null;
			timeout.slot = null;
			return next;
		}
	}
}
