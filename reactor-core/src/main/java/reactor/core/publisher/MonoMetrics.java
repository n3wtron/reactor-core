/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

import static reactor.core.publisher.FluxMetrics.*;

/**
 * Activate metrics gathering on a {@link Mono}, assumes Micrometer is on the classpath.
 *
 * @implNote Metrics.isInstrumentationAvailable() test should be performed BEFORE instantiating
 * or referencing this class, otherwise a {@link NoClassDefFoundError} will be thrown if
 * Micrometer is not there.
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
final class MonoMetrics<T> extends InternalMonoOperator<T, T> {

	final String        name;
	final Tags          tags;
	final MeterRegistry meterRegistry;
	final List<Double> percentiles;

	MonoMetrics(Mono<? extends T> mono, @Nullable List<Double> percentiles) {
		this(mono, null, percentiles);
	}

	MonoMetrics(Mono<? extends T> mono, @Nullable MeterRegistry meterRegistry) {
		this(mono, meterRegistry, null);
	}

	/**
	 * For testing purposes.
	 *
	 * @param meterRegistry the registry to use
	 */
	MonoMetrics(Mono<? extends T> mono, @Nullable MeterRegistry meterRegistry, @Nullable List<Double> percentiles) {
		super(mono);

		this.name = resolveName(mono);
		this.tags = resolveTags(mono, FluxMetrics.DEFAULT_TAGS_MONO, this.name);
		this.percentiles = percentiles;

		if (meterRegistry == null) {
			this.meterRegistry = Metrics.globalRegistry;
		}
		else {
			this.meterRegistry = meterRegistry;
		}
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new MetricsSubscriber<>(actual, meterRegistry, Clock.SYSTEM, this.tags, this.percentiles);
	}

	static class MetricsSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;
		final Clock                     clock;
		final Tags                      commonTags;
		final MeterRegistry             registry;
		final List<Double> 							percentiles;

		Timer.Sample subscribeToTerminateSample;
		boolean done;
		Subscription s;

		MetricsSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry, Clock clock, Tags commonTags, List<Double> percentiles) {
			this.actual = actual;
			this.clock = clock;
			this.commonTags = commonTags;
			this.registry = registry;
			this.percentiles = percentiles;
		}

		@Override
		final public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		final public void cancel() {
			FluxMetrics.recordCancel(commonTags, registry, subscribeToTerminateSample, percentiles);
			s.cancel();
		}

		@Override
		final public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			FluxMetrics.recordOnComplete(commonTags, registry, subscribeToTerminateSample, percentiles);
			actual.onComplete();
		}

		@Override
		final public void onError(Throwable e) {
			if (done) {
				FluxMetrics.recordMalformed(commonTags, registry);
				Operators.onErrorDropped(e, actual.currentContext());
				return;
			}
			done = true;
			FluxMetrics.recordOnError(commonTags, registry, subscribeToTerminateSample, percentiles, e);
			actual.onError(e);
		}

		@Override
		final public void onNext(T t) {
			if (done) {
				FluxMetrics.recordMalformed(commonTags, registry);
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			done = true;
			//TODO looks like we don't count onNext: `Mono.empty()` vs `Mono.just("foo")`
			FluxMetrics.recordOnComplete(commonTags, registry, subscribeToTerminateSample, percentiles);
			actual.onNext(t);
			actual.onComplete();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				FluxMetrics.recordOnSubscribe(commonTags, registry);
				this.subscribeToTerminateSample = Timer.start(clock);
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		final public void request(long l) {
			if (Operators.validate(l)) {
				s.request(l);
			}
		}
	}

}
