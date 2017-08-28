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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class FluxTimeoutTimedTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(3);

	@BeforeClass
	public static void beforeClass() {
		StepVerifier.setDefaultTimeout(TIMEOUT.plusSeconds(1));
	}

	@AfterClass
	public static void afterClass() {
		StepVerifier.resetDefaultTimeout();
	}

	@Test
	public void shouldNotTimeoutIfOnNextWithinTimeout() {
		TestPublisher<Integer> source = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> source.flux().timeout(TIMEOUT))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(() -> source.next(1))
		            .expectNext(1)
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(source::complete)
		            .verifyComplete();
	}

	@Test
	public void shouldNotTimeoutIfSecondOnNextWithinTimeout() {
		TestPublisher<Integer> source = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> source.flux().timeout(TIMEOUT))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(() -> source.next(1))
		            .thenAwait(Duration.ofSeconds(2))
		            .then(() -> source.next(2))
		            .expectNext(1, 2)
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(source::complete)
		            .verifyComplete();
	}

	@Test
	public void shouldTimeoutIfOnNextNotWithinTimeout() {
		TestPublisher<Integer> source = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> source.flux().timeout(TIMEOUT))
		            .expectSubscription()
		            .thenAwait(TIMEOUT.plusSeconds(1))
		            .verifyError(TimeoutException.class);
	}

	@Test
	public void shouldTimeoutIfSecondOnNextNotWithinTimeout() {
		TestPublisher<Integer> source = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> source.flux().timeout(TIMEOUT))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(() -> source.next(1))
		            .expectNext(1)
		            .expectNoEvent(TIMEOUT)
		            .thenAwait(Duration.ofSeconds(1))
		            .verifyError(TimeoutException.class);
	}

	@Test
	public void shouldCompleteIfUnderlyingCompletes() {
		TestPublisher<Integer> source = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> source.flux().timeout(TIMEOUT))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(source::complete)
		            .verifyComplete();
	}

	@Test
	public void shouldErrorIfUnderlyingError() {
		TestPublisher<Integer> source = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> source.flux().timeout(TIMEOUT))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(() -> source.error(new IllegalStateException("boom")))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void shouldSwitchToOtherIfOnNextNotWithinTimeout() {
		VirtualTimeScheduler testScheduler = VirtualTimeScheduler.create();
		TestPublisher<Integer> source = TestPublisher.create();
		Flux<Integer> other = Flux.just(100, 200, 300);
		Flux<Integer> test = source.flux().timeout(TIMEOUT, other, testScheduler);

		StepVerifier.withVirtualTime(() -> test, () -> testScheduler, Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(() -> source.next(1))
		            .thenAwait(Duration.ofSeconds(4))
		            .then(() -> source.next(2))
		            .expectNext(1, 100, 200, 300)
		            .verifyComplete();
	}

	@Test
	public void shouldSwitchToOtherIfOnErrorNotWithinTimeout() {
		VirtualTimeScheduler testScheduler = VirtualTimeScheduler.create();
		TestPublisher<Integer> source = TestPublisher.create();
		Flux<Integer> other = Flux.just(100, 200, 300);
		Flux<Integer> test = source.flux().timeout(TIMEOUT, other, testScheduler);

		StepVerifier.withVirtualTime(() -> test, () -> testScheduler, Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(() -> source.next(1))
		            .thenAwait(Duration.ofSeconds(4))
		            .then(() -> source.error(new IllegalStateException("boom")))
		            .expectNext(1, 100, 200, 300)
		            .verifyComplete();
	}

	@Test
	public void shouldSwitchToOtherIfOnCompletedNotWithinTimeout() {
		VirtualTimeScheduler testScheduler = VirtualTimeScheduler.create();
		TestPublisher<Integer> source = TestPublisher.create();
		Flux<Integer> other = Flux.just(100, 200, 300);
		Flux<Integer> test = source.flux().timeout(TIMEOUT, other, testScheduler);

		StepVerifier.withVirtualTime(() -> test, () -> testScheduler, Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .then(() -> source.next(1))
		            .thenAwait(Duration.ofSeconds(4))
		            .then(source::complete)
		            .expectNext(1, 100, 200, 300)
		            .verifyComplete();
	}

	@Test
	public void shouldSwitchToOtherAndCanBeUnsubscribedIfOnNextNotWithinTimeout() {
		VirtualTimeScheduler testScheduler = VirtualTimeScheduler.create();
		TestPublisher<String> source = TestPublisher.create();

		TestPublisher<String> other = TestPublisher.create();
		Flux<String> test = source.flux().timeout(TIMEOUT, other, testScheduler);

		AssertSubscriber<String> ts = new AssertSubscriber<>();

		test.subscribe(ts);

		testScheduler.advanceTimeBy(Duration.ofSeconds(2));
		source.next("One");
		testScheduler.advanceTimeBy(Duration.ofSeconds(4));
		source.next("Two");

		other.next("a","b");
		ts.cancel();

		// The following messages should not be delivered.
		other.emit("c", "d");

		ts.assertValueSequence(Arrays.asList("One", "a", "b"));
		ts.assertNotComplete();
		ts.assertNoError();
	}

	@Test
	public void shouldUnsubscribeFromUnderlyingSubscriptionOnTimeout() throws InterruptedException {
		AtomicInteger cancelled = new AtomicInteger();
		StepVerifier.withVirtualTime(() -> Flux.never()
		                                       .doOnCancel(cancelled::incrementAndGet)
		                                       .timeout(Duration.ofSeconds(1)))
		            .thenAwait(Duration.ofSeconds(2))
		            .expectError(TimeoutException.class)
		            .verify();

		assertThat(cancelled.get()).as("cancelled").isEqualTo(1);
	}

	@Test
	public void shouldUnsubscribeFromUnderlyingSubscriptionOnDispose() {
		TestPublisher<Integer> source = TestPublisher.create();
		VirtualTimeScheduler testScheduler = VirtualTimeScheduler.create();

		Disposable d = source.flux()
		                     .timeout(Duration.ofMillis(100), testScheduler)
		                     .subscribe();

		source.assertSubscribers();

		d.dispose();

		source.assertNoSubscribers();
	}

	@Test
	public void timedAndOther() {
		StepVerifier.withVirtualTime(() -> Flux.never()
		                                       .timeout(Duration.ofMillis(100), Flux.just(1)))
		            .thenAwait(Duration.ofSeconds(5))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void timedError() {
		StepVerifier.withVirtualTime(() -> Flux.error(new IllegalStateException("boom"))
		                                       .timeout(Duration.ofMinutes(1)))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void timedErrorOther() {
		StepVerifier.withVirtualTime(() -> Flux.error(new IllegalStateException("boom"))
		                                       .timeout(Duration.ofMinutes(1), Flux.just(1)))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void timedEmpty() {
		StepVerifier.withVirtualTime(() -> Flux.empty()
		                                       .timeout(Duration.ofMinutes(1)))
		            .verifyComplete();
	}

	@Test
	public void timedEmptyOther() {
		StepVerifier.withVirtualTime(() -> Flux.empty()
		                                       .timeout(Duration.ofMinutes(1), Flux.just(1)))
		            .verifyComplete();
	}

	@Test
	public void badSource() {
		TestPublisher<Integer> source = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(source.flux().timeout(Duration.ofDays(1)))
		            .expectSubscription()
		            .then(() -> {
			            source.next(1);
			            source.complete();
			            source.next(2);
			            source.error(new IllegalStateException("boom"));
			            source.complete();
		            })
		            .expectNext(1)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("boom")
		            .hasDroppedExactly(2)
		            .hasOperatorErrors(0);
	}

	@Test
	public void badSourceOther() {
		TestPublisher<Integer> source = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(source.flux().timeout(Duration.ofDays(1), Flux.just(100)))
		            .expectSubscription()
		            .then(() -> {
			            source.next(1);
			            source.complete();
			            source.next(2);
			            source.error(new IllegalStateException("boom"));
			            source.complete();
		            })
		            .expectNext(1)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("boom")
		            .hasDroppedExactly(2)
		            .hasOperatorErrors(0);
	}

	@Test
	public void timedTake() {
		TestPublisher<Integer> source = TestPublisher.create();

		StepVerifier.create(source.flux().timeout(Duration.ofDays(1)).take(1))
		            .expectSubscription()
		            .then(() -> source.assertSubscribers(1))
		            .then(() -> source.next(1))
		            .expectNext(1)
		            .then(source::assertNoSubscribers)
		            .verifyComplete();
	}

	@Test
	public void timedTakeOther() {
		TestPublisher<Integer> source = TestPublisher.create();

		StepVerifier.create(source.flux().timeout(Duration.ofDays(1), Flux.just(100)).take(1))
		            .expectSubscription()
		            .then(() -> source.assertSubscribers(1))
		            .then(() -> source.next(1))
		            .expectNext(1)
		            .then(source::assertNoSubscribers)
		            .verifyComplete();
	}

	Flux<Integer> scenario_timeoutCanBeBoundWithCallback() {
		return Flux.<Integer>never().timeout(Duration.ofMillis(500), Flux.just(-5));
	}

	@Test
	public void timeoutCanBeBoundWithCallback() {
		StepVerifier.withVirtualTime(this::scenario_timeoutCanBeBoundWithCallback)
		            .thenAwait(Duration.ofMillis(500))
		            .expectNext(-5)
		            .verifyComplete();
	}

	Flux<?> scenario_timeoutThrown() {
		return Flux.never()
		           .timeout(Duration.ofMillis(500));
	}

	@Test
	public void fluxPropagatesErrorUsingAwait() {
		StepVerifier.withVirtualTime(this::scenario_timeoutThrown)
		            .thenAwait(Duration.ofMillis(500))
		            .verifyError(TimeoutException.class);
	}

	Flux<Integer> scenario_timeoutCanBeBoundWithCallback2() {
		return Flux.<Integer>never().timeout(Duration.ofMillis(500), Flux.just(-5));
	}

	@Test
	public void timeoutCanBeBoundWithCallback2() {
		StepVerifier.withVirtualTime(this::scenario_timeoutCanBeBoundWithCallback2)
		            .thenAwait(Duration.ofMillis(500))
		            .expectNext(-5)
		            .verifyComplete();
	}

	Flux<?> scenario_timeoutThrown2() {
		return Flux.never()
		           .timeout(Duration.ofMillis(500));
	}

	@Test
	public void fluxPropagatesErrorUsingAwait2() {
		StepVerifier.withVirtualTime(this::scenario_timeoutThrown2)
		            .thenAwait(Duration.ofMillis(500))
		            .verifyError(TimeoutException.class);
	}

	Flux<?> scenario_timeoutThrown3() {
		return Flux.never()
		           .timeout(Duration.ofMillis(500), Schedulers.parallel());
	}

	@Test
	public void fluxPropagatesErrorUsingAwait3() {
		StepVerifier.withVirtualTime(this::scenario_timeoutThrown3)
		            .thenAwait(Duration.ofMillis(500))
		            .verifyError(TimeoutException.class);
	}


	@Test
	@Ignore("gh744 not fully clear on the expectation, still intermittently fails")
	public void gh744() {
		Map<String, Integer> failed = new HashMap<>();
		for (int i = 0; i < 50; i++) {
			ReplayProcessor<Integer> replayProcessorInstance = ReplayProcessor.create();

			Flux<Integer> test = replayProcessorInstance
					.filter(s -> true)
					.map(v -> v * 100)
					.timeout(Duration.ofMillis(100));

			try {
				StepVerifier.create(test)
				            .thenAwait(Duration.ofMillis(100))
				            .then(() -> {
					            replayProcessorInstance.onNext(1);
					            replayProcessorInstance.onNext(2);
					            replayProcessorInstance.onNext(3);
					            replayProcessorInstance.onComplete();
				            })
				            .as("iteration " + i)
				            .expectError(TimeoutException.class)
				            .verify();
			}
			catch (Throwable t) {
				if (failed.merge(t.toString(), 1, (a, b) -> a + b) == 1) {
					t.printStackTrace();
				}
			}
		}
		if (!failed.isEmpty()) {
			fail(failed.entrySet()
			           .stream()
			           .map(e -> "\n" + e.getValue() + " of " + e.getKey())
			           .reduce("", String::concat));
		}
	}
}
