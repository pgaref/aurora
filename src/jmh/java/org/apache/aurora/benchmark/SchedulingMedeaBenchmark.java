/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.benchmark;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import org.apache.aurora.benchmark.fakes.FakeDriver;
import org.apache.aurora.benchmark.fakes.FakeRescheduleCalculator;
import org.apache.aurora.benchmark.fakes.FakeStatsProvider;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.async.AsyncModule;
import org.apache.aurora.scheduler.async.DelayExecutor;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.mesos.TestExecutorSettings;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.offers.OfferSettings;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.preemptor.ClusterStateImpl;
import org.apache.aurora.scheduler.preemptor.PendingTaskProcessor;
import org.apache.aurora.scheduler.preemptor.PreemptorModule;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator;
import org.apache.aurora.scheduler.scheduling.TaskScheduler;
import org.apache.aurora.scheduler.scheduling.TaskScheduler.TaskSchedulerImpl.ReservationDuration;
import org.apache.aurora.scheduler.state.StateModule;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

/**
 * Performance benchmarks for the task scheduling loop.
 */
public class SchedulingMedeaBenchmark {

  /**
   * Constructs scheduler objects and populates offers/tasks for the benchmark run.
   */
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 1)
  @Measurement(iterations = 5)
  @Fork(1)
  @State(Scope.Thread)
  public abstract static class AbstractBase {
    private static final Amount<Long, Time> NO_DELAY = Amount.of(1L, Time.MILLISECONDS);
    private static final Amount<Long, Time> DELAY_FOREVER = Amount.of(30L, Time.DAYS);
    protected Storage storage;
    protected PendingTaskProcessor pendingTaskProcessor;
    private TaskScheduler taskScheduler;
    private OfferManager offerManager;
    private EventBus eventBus;
    private BenchmarkSettings settings;

    /**
     * Time for MEDEA to shine!
     * Runs once to setup up benchmark state.
     */
    @Setup(Level.Trial)
    public void setUpBenchmark() {
      storage = DbUtil.createStorage();
      eventBus = new EventBus();
      final FakeClock clock = new FakeClock();
      clock.setNowMillis(System.currentTimeMillis());

      // TODO(maxim): Find a way to DRY it and reuse existing modules instead.
      Injector injector = Guice.createInjector(
          new StateModule(),
          new PreemptorModule(true, NO_DELAY, NO_DELAY),
          new TierModule(TaskTestUtil.TIER_CONFIG),
          new PrivateModule() {
            @Override
            protected void configure() {
              // We use a no-op executor for async work, as this benchmark is focused on the
              // synchronous scheduling operations.
              bind(DelayExecutor.class).annotatedWith(AsyncModule.AsyncExecutor.class)
                  .toInstance(new DelayExecutor() {
                    @Override
                    public void execute(Runnable work, Amount<Long, Time> minDelay) {
                      // No-op.
                    }

                    @Override
                    public void execute(Runnable command) {
                      // No-op.
                    }
                  });
              bind(OfferManager.class).to(OfferManager.OfferManagerImpl.class);
              bind(OfferManager.OfferManagerImpl.class).in(Singleton.class);
              bind(OfferSettings.class).toInstance(
                  new OfferSettings(NO_DELAY, () -> DELAY_FOREVER));
              bind(BiCache.BiCacheSettings.class).toInstance(
                  new BiCache.BiCacheSettings(DELAY_FOREVER, ""));
              bind(TaskScheduler.class).to(TaskScheduler.TaskSchedulerImpl.class);
              bind(TaskScheduler.TaskSchedulerImpl.class).in(Singleton.class);
              expose(TaskScheduler.class);
              expose(OfferManager.class);
            }
          },
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(new TypeLiteral<Amount<Long, Time>>() { })
                  .annotatedWith(ReservationDuration.class)
                  .toInstance(DELAY_FOREVER);
              bind(TaskIdGenerator.class).to(TaskIdGenerator.TaskIdGeneratorImpl.class);
              bind(SchedulingFilter.class).to(SchedulingFilterImpl.class);
              bind(SchedulingFilterImpl.class).in(Singleton.class);
              bind(ExecutorSettings.class).toInstance(TestExecutorSettings.THERMOS_EXECUTOR);
              bind(Storage.class).toInstance(storage);
              bind(Driver.class).toInstance(new FakeDriver());
              bind(RescheduleCalculator.class).toInstance(new FakeRescheduleCalculator());
              bind(Clock.class).toInstance(clock);
              bind(StatsProvider.class).toInstance(new FakeStatsProvider());
              bind(EventSink.class).toInstance(eventBus::post);
              bind(IServerInfo.class).toInstance(IServerInfo.build(new ServerInfo("jmh", "")));
            }
          }
      );

      taskScheduler = injector.getInstance(TaskScheduler.class);
      offerManager = injector.getInstance(OfferManager.class);
      pendingTaskProcessor = injector.getInstance(PendingTaskProcessor.class);
      eventBus.register(injector.getInstance(ClusterStateImpl.class));

      settings = getSettings();
      saveHostAttributes(settings.getHostAttributes());

      Set<HostOffer> offers = new Offers.Builder().build(settings.getHostAttributes());
      Offers.addOffers(offerManager, offers);
      fillUpCluster(offers.size());

      saveTasks(settings.getTasks());
    }

    private Set<IScheduledTask> buildClusterTasks(int numOffers) {
      int numOffersToFill = (int) Math.round(numOffers * settings.getClusterUtilization());
      return new Tasks.Builder()
          .setRole("dummyTask")
          .setProduction(!settings.areAllVictimsEligibleForPreemption())
          .build(numOffersToFill);
    }

    private void fillUpCluster(int numOffers) {
      Set<IScheduledTask> tasksToAssign = buildClusterTasks(numOffers);
      System.out.println("Number of tasks to assign: "+ tasksToAssign.size());
      System.out.println("Tasks List: "+ tasksToAssign);
      saveTasks(tasksToAssign);
      storage.write((NoResult.Quiet) store -> {
        for (IScheduledTask scheduledTask : tasksToAssign) {
          taskScheduler.schedule(store, scheduledTask.getAssignedTask().getTaskId());
        }
      });
    }

    private void saveTasks(final Set<IScheduledTask> tasks) {
      storage.write(
          (NoResult.Quiet) storeProvider -> storeProvider.getUnsafeTaskStore().saveTasks(tasks));
    }

    private void saveHostAttributes(final Set<IHostAttributes> hostAttributesToSave) {
      storage.write((NoResult.Quiet) storeProvider -> {
        for (IHostAttributes attributes : hostAttributesToSave) {
          System.out.println("Host Attributes: "+ attributes);
          storeProvider.getAttributeStore().saveHostAttributes(attributes);
        }
      });
    }

    protected abstract BenchmarkSettings getSettings();

    /**
     * Benchmark entry point. All settings (e.g. iterations, benchmarkMode and etc.) are defined
     * in build.gradle.
     *
     * @return A "blackhole" to make sure the result is not optimized out.
     * See {@see http://openjdk.java.net/projects/code-tools/jmh/} for more info.
     */
    @Benchmark
    public boolean runBenchmark() {
      return storage.write((Storage.MutateWork.Quiet<Boolean>) store -> {
        boolean result = false;
        for (IScheduledTask task : settings.getTasks()) {
          result = taskScheduler.schedule(store, task.getAssignedTask().getTaskId());
        }
        return result;
      });
    }
  }

//  /**
//   * Tests scheduling performance with a task vetoed due to insufficient CPU.
//   */
//  public static class MedeaComparisonSchedulingBenchmark extends AbstractBase {
//    @Override
//    protected BenchmarkSettings getSettings() {
//      return new BenchmarkSettings.Builder()
//          .setHostAttributes(new Hosts.Builder().setNumHostsPerRack(100).build(100))
//          .setTasks(new Tasks.Builder()
//              .setProduction(true)
//              .setCpu(2)
//              .build(1))
//              .build();
//    }
//  }

  /**
   * Tests scheduling performance with a task vetoed due to limit constraint mismatch.
   */
  public static class LimitConstraintMismatchSchedulingBenchmark extends AbstractBase {
    @Override
    protected BenchmarkSettings getSettings() {
      return new BenchmarkSettings.Builder()
              .setHostAttributes(new Hosts.Builder()
                      .setNumHostsPerRack(10)
                      .build(10))
              .setTasks(new Tasks.Builder()
                      .setProduction(true)
                      .addLimitConstraint("host", 0)
                      .build(1)).build();
    }
  }

  public static void main(String[] args) {
    AbstractBase bench = new LimitConstraintMismatchSchedulingBenchmark();
    bench.setUpBenchmark();


//    bench.runBenchmark();

  }

}
