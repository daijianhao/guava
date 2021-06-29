/*
 * Copyright (C) 2012 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.util.concurrent;

import com.google.common.annotations.GwtIncompatible;
import com.google.common.math.LongMath;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;

@GwtIncompatible
@ElementTypesAreNonnullByDefault
abstract class SmoothRateLimiter extends RateLimiter {
    /*
     * How is the RateLimiter designed, and why?
     *
     * The primary feature of a RateLimiter is its "stable rate", the maximum rate that it should
     * allow in normal conditions. This is enforced by "throttling" incoming requests as needed. For
     * example, we could compute the appropriate throttle time for an incoming request, and make the
     * calling thread wait for that time.
     *
     * The simplest way to maintain a rate of QPS is to keep the timestamp of the last granted
     * request, and ensure that (1/QPS) seconds have elapsed since then. For example, for a rate of
     * QPS=5 (5 tokens per second), if we ensure that a request isn't granted earlier than 200ms after
     * the last one, then we achieve the intended rate. If a request comes and the last request was
     * granted only 100ms ago, then we wait for another 100ms. At this rate, serving 15 fresh permits
     * (i.e. for an acquire(15) request) naturally takes 3 seconds.
     *
     * It is important to realize that such a RateLimiter has a very superficial memory of the past:
     * it only remembers the last request. What if the RateLimiter was unused for a long period of
     * time, then a request arrived and was immediately granted? This RateLimiter would immediately
     * forget about that past underutilization. This may result in either underutilization or
     * overflow, depending on the real world consequences of not using the expected rate.
     *
     * Past underutilization could mean that excess resources are available. Then, the RateLimiter
     * should speed up for a while, to take advantage of these resources. This is important when the
     * rate is applied to networking (limiting bandwidth), where past underutilization typically
     * translates to "almost empty buffers", which can be filled immediately.
     *
     * On the other hand, past underutilization could mean that "the server responsible for handling
     * the request has become less ready for future requests", i.e. its caches become stale, and
     * requests become more likely to trigger expensive operations (a more extreme case of this
     * example is when a server has just booted, and it is mostly busy with getting itself up to
     * speed).
     *
     * To deal with such scenarios, we add an extra dimension, that of "past underutilization",
     * modeled by "storedPermits" variable. This variable is zero when there is no underutilization,
     * and it can grow up to maxStoredPermits, for sufficiently large underutilization. So, the
     * requested permits, by an invocation acquire(permits), are served from:
     *
     * - stored permits (if available)
     *
     * - fresh permits (for any remaining permits)
     *
     * How this works is best explained with an example:
     *
     * For a RateLimiter that produces 1 token per second, every second that goes by with the
     * RateLimiter being unused, we increase storedPermits by 1. Say we leave the RateLimiter unused
     * for 10 seconds (i.e., we expected a request at time X, but we are at time X + 10 seconds before
     * a request actually arrives; this is also related to the point made in the last paragraph), thus
     * storedPermits becomes 10.0 (assuming maxStoredPermits >= 10.0). At that point, a request of
     * acquire(3) arrives. We serve this request out of storedPermits, and reduce that to 7.0 (how
     * this is translated to throttling time is discussed later). Immediately after, assume that an
     * acquire(10) request arriving. We serve the request partly from storedPermits, using all the
     * remaining 7.0 permits, and the remaining 3.0, we serve them by fresh permits produced by the
     * rate limiter.
     *
     * We already know how much time it takes to serve 3 fresh permits: if the rate is
     * "1 token per second", then this will take 3 seconds. But what does it mean to serve 7 stored
     * permits? As explained above, there is no unique answer. If we are primarily interested to deal
     * with underutilization, then we want stored permits to be given out /faster/ than fresh ones,
     * because underutilization = free resources for the taking. If we are primarily interested to
     * deal with overflow, then stored permits could be given out /slower/ than fresh ones. Thus, we
     * require a (different in each case) function that translates storedPermits to throttling time.
     *
     * This role is played by storedPermitsToWaitTime(double storedPermits, double permitsToTake). The
     * underlying model is a continuous function mapping storedPermits (from 0.0 to maxStoredPermits)
     * onto the 1/rate (i.e. intervals) that is effective at the given storedPermits. "storedPermits"
     * essentially measure unused time; we spend unused time buying/storing permits. Rate is
     * "permits / time", thus "1 / rate = time / permits". Thus, "1/rate" (time / permits) times
     * "permits" gives time, i.e., integrals on this function (which is what storedPermitsToWaitTime()
     * computes) correspond to minimum intervals between subsequent requests, for the specified number
     * of requested permits.
     *
     * Here is an example of storedPermitsToWaitTime: If storedPermits == 10.0, and we want 3 permits,
     * we take them from storedPermits, reducing them to 7.0, and compute the throttling for these as
     * a call to storedPermitsToWaitTime(storedPermits = 10.0, permitsToTake = 3.0), which will
     * evaluate the integral of the function from 7.0 to 10.0.
     *
     * Using integrals guarantees that the effect of a single acquire(3) is equivalent to {
     * acquire(1); acquire(1); acquire(1); }, or { acquire(2); acquire(1); }, etc, since the integral
     * of the function in [7.0, 10.0] is equivalent to the sum of the integrals of [7.0, 8.0], [8.0,
     * 9.0], [9.0, 10.0] (and so on), no matter what the function is. This guarantees that we handle
     * correctly requests of varying weight (permits), /no matter/ what the actual function is - so we
     * can tweak the latter freely. (The only requirement, obviously, is that we can compute its
     * integrals).
     *
     * Note well that if, for this function, we chose a horizontal line, at height of exactly (1/QPS),
     * then the effect of the function is non-existent: we serve storedPermits at exactly the same
     * cost as fresh ones (1/QPS is the cost for each). We use this trick later.
     *
     * If we pick a function that goes /below/ that horizontal line, it means that we reduce the area
     * of the function, thus time. Thus, the RateLimiter becomes /faster/ after a period of
     * underutilization. If, on the other hand, we pick a function that goes /above/ that horizontal
     * line, then it means that the area (time) is increased, thus storedPermits are more costly than
     * fresh permits, thus the RateLimiter becomes /slower/ after a period of underutilization.
     *
     * Last, but not least: consider a RateLimiter with rate of 1 permit per second, currently
     * completely unused, and an expensive acquire(100) request comes. It would be nonsensical to just
     * wait for 100 seconds, and /then/ start the actual task. Why wait without doing anything? A much
     * better approach is to /allow/ the request right away (as if it was an acquire(1) request
     * instead), and postpone /subsequent/ requests as needed. In this version, we allow starting the
     * task immediately, and postpone by 100 seconds future requests, thus we allow for work to get
     * done in the meantime instead of waiting idly.
     *
     * This has important consequences: it means that the RateLimiter doesn't remember the time of the
     * _last_ request, but it remembers the (expected) time of the _next_ request. This also enables
     * us to tell immediately (see tryAcquire(timeout)) whether a particular timeout is enough to get
     * us to the point of the next scheduling time, since we always maintain that. And what we mean by
     * "an unused RateLimiter" is also defined by that notion: when we observe that the
     * "expected arrival time of the next request" is actually in the past, then the difference (now -
     * past) is the amount of time that the RateLimiter was formally unused, and it is that amount of
     * time which we translate to storedPermits. (We increase storedPermits with the amount of permits
     * that would have been produced in that idle time). So, if rate == 1 permit per second, and
     * arrivals come exactly one second after the previous, then storedPermits is _never_ increased --
     * we would only increase it for arrivals _later_ than the expected one second.
     */

    /**
     * This implements the following function where coldInterval = coldFactor * stableInterval.
     *
     * <pre>
     *          ^ throttling
     *          |
     *    cold  +                  /
     * interval |                 /.
     *          |                / .
     *          |               /  .   ← "warmup period" is the area of the trapezoid between
     *          |              /   .     thresholdPermits and maxPermits
     *          |             /    .
     *          |            /     .
     *          |           /      .
     *   stable +----------/  WARM .
     * interval |          .   UP  .
     *          |          . PERIOD.
     *          |          .       .
     *        0 +----------+-------+--------------→ storedPermits
     *          0 thresholdPermits maxPermits
     * </pre>
     * <p>
     * Before going into the details of this particular function, let's keep in mind the basics:
     *
     * <ol>
     *   <li>The state of the RateLimiter (storedPermits) is a vertical line in this figure.
     *   <li>When the RateLimiter is not used, this goes right (up to maxPermits)
     *   <li>When the RateLimiter is used, this goes left (down to zero), since if we have
     *       storedPermits, we serve from those first
     *   <li>When _unused_, we go right at a constant rate! The rate at which we move to the right is
     *       chosen as maxPermits / warmupPeriod. This ensures that the time it takes to go from 0 to
     *       maxPermits is equal to warmupPeriod.
     *   <li>When _used_, the time it takes, as explained in the introductory class note, is equal to
     *       the integral of our function, between X permits and X-K permits, assuming we want to
     *       spend K saved permits.
     * </ol>
     *
     * <p>In summary, the time it takes to move to the left (spend K permits), is equal to the area of
     * the function of width == K.
     *
     * <p>Assuming we have saturated demand, the time to go from maxPermits to thresholdPermits is
     * equal to warmupPeriod. And the time to go from thresholdPermits to 0 is warmupPeriod/2. (The
     * reason that this is warmupPeriod/2 is to maintain the behavior of the original implementation
     * where coldFactor was hard coded as 3.)
     *
     * <p>It remains to calculate thresholdsPermits and maxPermits.
     *
     * <ul>
     *   <li>The time to go from thresholdPermits to 0 is equal to the integral of the function
     *       between 0 and thresholdPermits. This is thresholdPermits * stableIntervals. By (5) it is
     *       also equal to warmupPeriod/2. Therefore
     *       <blockquote>
     *       thresholdPermits = 0.5 * warmupPeriod / stableInterval
     *       </blockquote>
     *   <li>The time to go from maxPermits to thresholdPermits is equal to the integral of the
     *       function between thresholdPermits and maxPermits. This is the area of the pictured
     *       trapezoid, and it is equal to 0.5 * (stableInterval + coldInterval) * (maxPermits -
     *       thresholdPermits). It is also equal to warmupPeriod, so
     *       <blockquote>
     *       maxPermits = thresholdPermits + 2 * warmupPeriod / (stableInterval + coldInterval)
     *       </blockquote>
     * </ul>
     * <p>
     * SmoothWarmingUp实现预热缓冲的关键在于其分发令牌的速率会随时间和令牌数而改变，速率会先慢后快。
     * 表现形式如下图所示，令牌刷新的时间间隔由长逐渐变短。等存储令牌数从maxPermits到达thresholdPermits时，
     * 发放令牌的时间价格也由coldInterval降低到了正常的stableInterval。
     */
    static final class SmoothWarmingUp extends SmoothRateLimiter {
        /**
         * 预热周期
         */
        private final long warmupPeriodMicros;
        /**
         * The slope of the line from the stable interval (when permits == 0), to the cold interval
         * (when permits == maxPermits)
         */
        private double slope;

      /**
       * 阈值
       */
      private double thresholdPermits;
        private double coldFactor;

        SmoothWarmingUp(
                SleepingStopwatch stopwatch, long warmupPeriod, TimeUnit timeUnit, double coldFactor) {
            super(stopwatch);
            this.warmupPeriodMicros = timeUnit.toMicros(warmupPeriod);
            this.coldFactor = coldFactor;
        }

        @Override
        void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
            //桶中最大令牌数
            double oldMaxPermits = maxPermits;
            //冷启动时的每个permit产生的时间，时间越长，则产生越慢，这里的 coldFactor 大于1的
            double coldIntervalMicros = stableIntervalMicros * coldFactor;
            //阈值，等于在预热时间段内可发放的令牌数的一半
            thresholdPermits = 0.5 * warmupPeriodMicros / stableIntervalMicros;
            //最大令牌数 = 阈值 + 梯形的高
            maxPermits =
                    thresholdPermits + 2.0 * warmupPeriodMicros / (stableIntervalMicros + coldIntervalMicros);
            //斜率，即IntervalMicros 在单位permit上变化的速度
            slope = (coldIntervalMicros - stableIntervalMicros) / (maxPermits - thresholdPermits);
            if (oldMaxPermits == Double.POSITIVE_INFINITY) {
                // if we don't special-case this, we would get storedPermits == NaN, below
                storedPermits = 0.0;
            } else {
                storedPermits =
                        (oldMaxPermits == 0.0)
                                ? maxPermits // initial state is cold
                                : storedPermits * maxPermits / oldMaxPermits;
            }
        }

        /**
         * 等待时间就是计算上图中梯形或者正方形的面积。
         * <pre>
         *          ^ throttling
         *          |
         *    cold  +                  /
         * interval |                 /.
         *          |                / .
         *          |               /  .   ← "warmup period" is the area of the trapezoid between
         *          |              /   .     thresholdPermits and maxPermits
         *          |             /    .
         *          |            /     .
         *          |           /      .
         *   stable +----------/  WARM .
         * interval |          .   UP  .
         *          |          . PERIOD.
         *          |          .       .
         *        0 +----------+-------+--------------→ storedPermits
         *          0 thresholdPermits maxPermits
         * </pre>
         *
         * @param storedPermits 当前桶中有的令牌数
         * @param permitsToTake 当前要消费的令牌数，不超过 storedPermits
         */
        @Override
        long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
            /**
             * 当前permits超出阈值的部分
             */
            double availablePermitsAboveThreshold = storedPermits - thresholdPermits;
            long micros = 0;
            // measuring the integral on the right part of the function (the climbing line)
            /**
             * 如果当前存储的令牌数超出thresholdPermits
             * 说明在预热阶段
             */
            if (availablePermitsAboveThreshold > 0.0) {
                /**
                 * 在阈值右侧并且需要被消耗的令牌数量
                 */
                double permitsAboveThresholdToTake = min(availablePermitsAboveThreshold, permitsToTake);

                /**
                 * 梯形的面积
                 *
                 * 高 * (顶 * 底) / 2
                 *
                 * 高是 permitsAboveThresholdToTake 也就是右侧需要消费的令牌数
                 * 底 较长 permitsToTime(availablePermitsAboveThreshold)
                 * 顶 较短 permitsToTime(availablePermitsAboveThreshold - permitsAboveThresholdToTake)
                 */
                // TODO(cpovirk): Figure out a good name for this variable.
                double length =
                        permitsToTime(availablePermitsAboveThreshold)
                                + permitsToTime(availablePermitsAboveThreshold - permitsAboveThresholdToTake);
                //面积，在预热阶段等待的时间
                micros = (long) (permitsAboveThresholdToTake * length / 2.0);
                /**
                 * 减去已经获取的在阈值右侧的令牌数
                 */
                permitsToTake -= permitsAboveThresholdToTake;
            }
            // measuring the integral on the left part of the function (the horizontal line)
            /**
             * 平稳时期的面积，正好是长乘宽
             * 在预热阶段之前需要等待的时间
             */
            micros += (long) (stableIntervalMicros * permitsToTake);
            return micros;
        }

        private double permitsToTime(double permits) {
            return stableIntervalMicros + permits * slope;
        }

        @Override
        double coolDownIntervalMicros() {
            /**
             * 每秒增加的令牌数为 warmup时间/maxPermits. 这样的话，在warmuptime时间内，就就增张的令牌数量
             * 为 maxPermits
             * 这里相当于计算 在预热周时间段内，每增加一个permits需要的时间
             *
             * 这个rate是 maxPermits / warmupPeriod.
             * 这样就能保证从0移动到maxPermits的时间等于warmupPeriod
             */
            return warmupPeriodMicros / maxPermits;
        }
    }

    /**
     * This implements a "bursty" RateLimiter, where storedPermits are translated to zero throttling.
     * The maximum number of permits that can be saved (when the RateLimiter is unused) is defined in
     * terms of time, in this sense: if a RateLimiter is 2qps, and this time is specified as 10
     * seconds, we can save up to 2 * 10 = 20 permits.
     */
    static final class SmoothBursty extends SmoothRateLimiter {
        /**
         * The work (permits) of how many seconds can be saved up if this RateLimiter is unused?
         * bukect最大的长度 秒
         */
        final double maxBurstSeconds;

        SmoothBursty(SleepingStopwatch stopwatch, double maxBurstSeconds) {
            super(stopwatch);
            this.maxBurstSeconds = maxBurstSeconds;
        }

        @Override
        void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
            double oldMaxPermits = this.maxPermits;
            //每个每周内允许通过的数量
            maxPermits = maxBurstSeconds * permitsPerSecond;
            if (oldMaxPermits == Double.POSITIVE_INFINITY) {
                // if we don't special-case this, we would get storedPermits == NaN, below
                storedPermits = maxPermits;
            } else {
                storedPermits =
                        (oldMaxPermits == 0.0)
                                ? 0.0 // initial state
                                : storedPermits * maxPermits / oldMaxPermits;
            }
        }

        @Override
        long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
            return 0L;
        }

        @Override
        double coolDownIntervalMicros() {
            return stableIntervalMicros;
        }
    }

    /**
     * The currently stored permits.
     * 当前存储令牌数
     */
    double storedPermits;

    /**
     * The maximum number of stored permits.
     * 最大存储令牌数
     */
    double maxPermits;

    /**
     * The interval between two unit requests, at our stable rate. E.g., a stable rate of 5 permits
     * per second has a stable interval of 200ms.
     * <p>
     * 每个请求间的间隔
     */

    double stableIntervalMicros;

    /**
     * The time when the next request (no matter its size) will be granted. After granting a request,
     * this is pushed further in the future. Large requests push this further than small requests.
     * 下一次请求可以获取令牌的起始时间
     * 由于RateLimiter允许预消费，上次请求预消费令牌后
     * 下次请求需要等待相应的时间到nextFreeTicketMicros时刻才可以获取令牌
     */
    private long nextFreeTicketMicros = 0L; // could be either in the past or future

    private SmoothRateLimiter(SleepingStopwatch stopwatch) {
        super(stopwatch);
    }

    @Override
    final void doSetRate(double permitsPerSecond, long nowMicros) {
        resync(nowMicros);
        //计算每多少微秒通过一个
        double stableIntervalMicros = SECONDS.toMicros(1L) / permitsPerSecond;
        this.stableIntervalMicros = stableIntervalMicros;
        doSetRate(permitsPerSecond, stableIntervalMicros);
    }

    abstract void doSetRate(double permitsPerSecond, double stableIntervalMicros);

    @Override
    final double doGetRate() {
        return SECONDS.toMicros(1L) / stableIntervalMicros;
    }

    @Override
    final long queryEarliestAvailable(long nowMicros) {
        return nextFreeTicketMicros;
    }

    /**
     * reserveEarliestAvailable是刷新令牌数和下次获取令牌时间 nextFreeTicketMicros的关键函数。
     * 它有三个步骤，
     * 一是调用 resync函数增加令牌数，
     * 二是计算预支付令牌所需额外等待的时间，
     * 三是更新下次获取令牌时间 nextFreeTicketMicros和存储令牌数 storedPermits
     * <p>
     * 这里涉及 RateLimiter的一个特性，也就是可以预先支付令牌，并且所需等待的时间在下次获取令牌时再实际执行
     */
    @Override
    final long reserveEarliestAvailable(int requiredPermits, long nowMicros) {
        // 刷新令牌数，相当于每次acquire时在根据时间进行令牌的刷新
        resync(nowMicros);
        long returnValue = nextFreeTicketMicros;
        // 获取当前已有的令牌数和需要获取的目标令牌数进行比较，计算出可以目前即可得到的令牌数。
        double storedPermitsToSpend = min(requiredPermits, this.storedPermits);
        // freshPermits是需要预先支付的令牌，也就是目标令牌数减去目前即可得到的令牌数
        double freshPermits = requiredPermits - storedPermitsToSpend;
        // 因为会突然涌入大量请求，而现有令牌数又不够用，因此会预先支付一定的令牌数
        // waitMicros即是产生预先支付令牌的数量时间，则将下次要添加令牌的时间应该计算时间加上watiMicros
        long waitMicros =
                storedPermitsToWaitTime(this.storedPermits, storedPermitsToSpend)
                        + (long) (freshPermits * stableIntervalMicros);
        // 更新nextFreeTicketMicros,本次预先支付的令牌所需等待的时间让下一次请求来实际等待。
        this.nextFreeTicketMicros = LongMath.saturatedAdd(nextFreeTicketMicros, waitMicros);
        // 更新令牌数，最低数量为0
        this.storedPermits -= storedPermitsToSpend;
        // 返回旧的nextFreeTicketMicros数值，无需为预支付的令牌多加等待时间。
        return returnValue;
    }

    /**
     * Translates a specified portion of our currently stored permits which we want to spend/acquire,
     * into a throttling time. Conceptually, this evaluates the integral of the underlying function we
     * use, for the range of [(storedPermits - permitsToTake), storedPermits].
     *
     * <p>This always holds: {@code 0 <= permitsToTake <= storedPermits}
     *
     * @param storedPermits 当前桶中有的令牌数
     * @param permitsToTake 当前要消费的令牌数，不超过 storedPermits
     */
    abstract long storedPermitsToWaitTime(double storedPermits, double permitsToTake);

    /**
     * Returns the number of microseconds during cool down that we have to wait to get a new permit.
     */
    abstract double coolDownIntervalMicros();

    /**
     * Updates {@code storedPermits} and {@code nextFreeTicketMicros} based on the current time.
     */
    void resync(long nowMicros) {
        // if nextFreeTicket is in the past, resync to now
        if (nowMicros > nextFreeTicketMicros) {//当前时间已经超过了下次可以获取令牌的时间
            //超过部分的时间 / 每个请求的间隔时间 = 这部分时间累计的令牌数
            double newPermits = (nowMicros - nextFreeTicketMicros) / coolDownIntervalMicros();
            //当前累计的令牌数
            storedPermits = min(maxPermits, storedPermits + newPermits);
            //更新下次可获取令牌的时间
            nextFreeTicketMicros = nowMicros;
        }
    }
}
