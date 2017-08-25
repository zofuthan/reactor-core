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

import org.reactivestreams.Publisher;
import reactor.core.scheduler.Scheduler;

/**
 * @author Simon Baslé
 * @author David Karnok
 */
class FluxTimeoutTimed<T> extends FluxTimeoutOther<T, Long, Long> {

	public FluxTimeoutTimed(Flux<T> source, Duration timeout, Scheduler timer) {
		super(source,
				Mono.delay(timeout, timer).onErrorReturn(0L),
				o -> Mono.delay(timeout, timer).onErrorReturn(0L)
		);
	}

	public FluxTimeoutTimed(Flux<T> source, Duration timeout, Scheduler timer,
			Publisher<? extends T> fallback) {
		super(source,
				Mono.delay(timeout, timer).onErrorReturn(0L),
				o -> Mono.delay(timeout, timer).onErrorReturn(0L),
				fallback
		);
	}
}