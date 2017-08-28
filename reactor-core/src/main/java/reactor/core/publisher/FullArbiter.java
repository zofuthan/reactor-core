/*
 * Copyright (c) 2016-present, RxJava Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiPredicate;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.util.concurrent.Queues;

/**
 * Performs full arbitration of Subscriber events with strict drain (i.e., old emissions of another
 * subscriber are dropped).
 *
 * @param <T> the value type
 * @author David Karnok
 */
public final class FullArbiter<T> extends FullArbiterPad2 implements Subscription {

	final Subscriber<? super T>         actual;
	final BiPredicate<Object, Object>   queue;

	long requested;

	volatile Subscription s;
	static final Subscription INITIAL = new InitialSubscription();

	Disposable resource;

	volatile boolean cancelled;

	static final Object REQUEST = new Object();

	public FullArbiter(Subscriber<? super T> actual, Disposable resource, int capacity) {
		this.actual = actual;
		this.resource = resource;
		Queue<Object> q = Queues.unbounded(capacity).get();
		//noinspection unchecked
		this.queue = (BiPredicate<Object, Object>) q;
		this.s = INITIAL;
	}

	@Override
	public void request(long n) {
		if (Operators.validate(n)) {
			Operators.getAndAddCap(MISSED_REQUESTED, this, n);
			queue.test(REQUEST, REQUEST);
			drain();
		}
	}

	@Override
	public void cancel() {
		if (!cancelled) {
			cancelled = true;
			dispose();
		}
	}

	void dispose() {
		Disposable d = resource;
		resource = null;
		if (d != null) {
			d.dispose();
		}
	}

	public boolean setSubscription(Subscription s) {
		if (cancelled) {
			if (s != null) {
				s.cancel();
			}
			return false;
		}

		Objects.requireNonNull(s, "s is null");
		queue.test(this.s, Signal.subscribe(s));
		drain();
		return true;
	}

	public boolean onNext(T value, Subscription s) {
		if (cancelled) {
			return false;
		}

		queue.test(s, Signal.next(value));
		drain();
		return !cancelled;
	}

	public void onError(Throwable value, Subscription s) {
		if (cancelled) {
			Operators.onErrorDropped(value);
			return;
		}
		queue.test(s, Signal.error(value));
		drain();
	}

	public void onComplete(Subscription s) {
		queue.test(s, Signal.complete());
		drain();
	}

	void drain() {
		if (wip.getAndIncrement() != 0) {
			return;
		}

		int missed = 1;

		//noinspection unchecked
		final Queue<Object> q = (Queue<Object>) queue;
		final Subscriber<? super T> a = actual;

		for (;;) {

			for (;;) {

				Object o = q.poll();
				if (o == null) {
					break;
				}
				Object v = q.poll();

				if (o == REQUEST) {
					long mr = MISSED_REQUESTED.getAndSet(this,0L);
					if (mr != 0L) {
						requested = Operators.addCap(requested, mr);
						s.request(mr);
					}
				} else
				if (o == s) {
					Signal<T> signal;
					if (!(v instanceof Signal)) {
						signal = Signal.error(new IllegalStateException("While polling, expected a Signal but found " + v));
					}
					else {
						//noinspection unchecked
						signal = (Signal<T>) v;
					}

					if (signal.isOnSubscribe()) {
						Subscription next = signal.getSubscription();
						if (!cancelled) {
							s = next;
							long r = requested;
							if (r != 0L) {
								next.request(r);
							}
						} else {
							next.cancel();
						}
					} else if (signal.isOnError()) {
						q.clear();
						dispose();

						Throwable ex = signal.getThrowable();
						if (!cancelled) {
							cancelled = true;
							a.onError(ex);
						} else {
							Operators.onErrorDropped(ex);
						}
					} else if (signal.isOnComplete()) {
						q.clear();
						dispose();

						if (!cancelled) {
							cancelled = true;
							a.onComplete();
						}
					} else {
						long r = requested;
						if (r != 0) {
							a.onNext(signal.get());
							requested = r - 1;
						}
					}
				}
			}

			missed = wip.addAndGet(-missed);
			if (missed == 0) {
				break;
			}
		}
	}

	static final class InitialSubscription implements Subscription {
		@Override
		public void request(long n) {
			// deliberately no op
		}

		@Override
		public void cancel() {
			// deliberately no op
		}
	}
}

/** Pads the object header away. */
class FullArbiterPad0 {
	volatile long p1a, p2a, p3a, p4a, p5a, p6a, p7a;
	volatile long p8a, p9a, p10a, p11a, p12a, p13a, p14a, p15a;
}

/** The work-in-progress counter. */
class FullArbiterWip extends FullArbiterPad0 {
	final AtomicInteger wip = new AtomicInteger();
}

/** Pads the wip counter away. */
class FullArbiterPad1 extends FullArbiterWip {
	volatile long p1b, p2b, p3b, p4b, p5b, p6b, p7b;
	volatile long p8b, p9b, p10b, p11b, p12b, p13b, p14b, p15b;
}

/** The missed request counter. */
class FullArbiterMissed extends FullArbiterPad1 {
	volatile long missedRequested;
	static AtomicLongFieldUpdater<FullArbiterMissed> MISSED_REQUESTED =
			AtomicLongFieldUpdater.newUpdater(FullArbiterMissed.class, "missedRequested");
}

/** Pads the missed request counter away. */
class FullArbiterPad2 extends FullArbiterMissed {
	volatile long p1c, p2c, p3c, p4c, p5c, p6c, p7c;
	volatile long p8c, p9c, p10c, p11c, p12c, p13c, p14c, p15c;
}
