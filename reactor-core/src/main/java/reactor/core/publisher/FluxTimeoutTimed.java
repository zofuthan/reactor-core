/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/**
 * @author Simon Baslé
 * @author David Karnok
 */
class FluxTimeoutTimed<T> extends FluxOperator<T, T> {

	static final Disposable NEW_TIMER = Disposable.disposed();

	final Duration timeout;
	final Scheduler timer;
	final Publisher<? extends T> fallback;

	public FluxTimeoutTimed(Flux<? extends T> source, Duration timeout, Scheduler timer) {
		super(source);
		this.timeout = timeout;
		this.timer = timer;
		this.fallback = null;
	}

	public FluxTimeoutTimed(Flux<? extends T> source, Duration timeout, Scheduler timer,
			Publisher<? extends T> fallback) {
		super(source);
		this.timeout = timeout;
		this.timer = timer;
		this.fallback = fallback;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> s) {
		if (fallback == null) {
			source.subscribe(new TimeoutTimedSubscriber<T>(
					new SerializedSubscriber<>(s), // because errors can race
					timeout, timer.createWorker()));
		} else {
			source.subscribe(new TimeoutTimedOtherSubscriber<T>(
					s, // the FullArbiter serializes
					timeout, timer.createWorker(), fallback));
		}
	}

	static final class TimeoutTimedOtherSubscriber<T> implements InnerOperator<T, T>, Disposable {
		final CoreSubscriber<? super T> actual;
		final Duration timeout;
		final Scheduler.Worker worker;
		final Publisher<? extends T> fallback;

		Subscription s;

		final FullArbiter<T> arbiter;

		Disposable timer;

		volatile long index;

		volatile boolean done;

		TimeoutTimedOtherSubscriber(CoreSubscriber<? super T> actual, Duration timeout, Scheduler.Worker worker,
				Publisher<? extends T> fallback) {
			this.actual = actual;
			this.timeout = timeout;
			this.worker = worker;
			this.fallback = fallback;
			this.arbiter = new FullArbiter<T>(actual, this, 8);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				if (arbiter.setSubscription(s)) {
					actual.onSubscribe(arbiter);

					scheduleTimeout(0L);
				}
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				return;
			}
			long idx = index + 1;
			index = idx;

			if (arbiter.onNext(t, s)) {
				scheduleTimeout(idx);
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long l) {
			//FIXME
		}

		@Override
		public void cancel() {
			s.cancel();
			worker.dispose();
		}

		void scheduleTimeout(final long idx) {
			if (timer != null) {
				timer.dispose();
			}

			timer = worker.schedule(new TimeoutTask(idx), timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		void subscribeNext() {
			fallback.subscribe(new FullArbiterSubscriber<>(arbiter));
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			arbiter.onError(t, s);
			worker.dispose();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			arbiter.onComplete(s);
			worker.dispose();
		}

		@Override
		public void dispose() {
			cancel();
		}

		@Override
		public boolean isDisposed() {
			return worker.isDisposed();
		}

		final class TimeoutTask implements Runnable {
			private final long idx;

			TimeoutTask(long idx) {
				this.idx = idx;
			}

			@Override
			public void run() {
				if (idx == index) {
					done = true;
					s.cancel();
					worker.dispose();

					subscribeNext();

				}
			}
		}
	}

	static final class TimeoutTimedSubscriber<T> implements InnerOperator<T, T>, Disposable, Subscription {
		final CoreSubscriber<? super T> actual;
		final Duration timeout;
		final Scheduler.Worker worker;

		Subscription s;

		Disposable timer;

		volatile long index;

		volatile boolean done;

		TimeoutTimedSubscriber(CoreSubscriber<? super T> actual, Duration timeout, Scheduler.Worker worker) {
			this.actual = actual;
			this.timeout = timeout;
			this.worker = worker;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
				scheduleTimeout(0L);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			long idx = index + 1;
			index = idx;

			actual.onNext(t);

			scheduleTimeout(idx);
		}

		void scheduleTimeout(final long idx) {
			if (timer != null) {
				timer.dispose();
			}

			timer = worker.schedule(new TimeoutTask(idx), timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;

			actual.onError(t);
			worker.dispose();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			actual.onComplete();
			worker.dispose();
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void dispose() {
			s.cancel();
			worker.dispose();
		}

		@Override
		public boolean isDisposed() {
			return worker.isDisposed();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			dispose();
		}

		final class TimeoutTask implements Runnable {
			private final long idx;

			TimeoutTask(long idx) {
				this.idx = idx;
			}

			@Override
			public void run() {
				if (idx == index) {
					done = true;
					dispose();

					actual.onError(new TimeoutException());
				}
			}
		}
	}

}
