package com.hlf.batchchunk;

import org.springframework.batch.core.scope.context.JobSynchronizationManager;
import org.springframework.core.task.AsyncTaskExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class JobScopeExecutorDecorator implements AsyncTaskExecutor {
  private final AsyncTaskExecutor delegate;

  JobScopeExecutorDecorator(AsyncTaskExecutor delegate) {
    this.delegate = delegate;
  }

  @Override
  public void execute(Runnable task, long startTimeout) {
    delegate.execute(wrap(task), startTimeout);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegate.submit(wrap(task));
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegate.submit(wrap(task));
  }

  @Override
  public void execute(Runnable task) {
    delegate.execute(wrap(task));
  }

  protected final Runnable wrap(Runnable delegate) {
    return JobScopeDelegatingRunnable.create(delegate);
  }

  protected final <T> Callable<T> wrap(Callable<T> delegate) {
    return JobScopeDelegatingCallable.create(delegate);
  }

  public static class JobScopeDelegatingRunnable {
    static Runnable create(Runnable delegate) {
      var originalExecution = JobSynchronizationManager.getContext().getJobExecution();
      return new Runnable() {
        @Override
        public void run() {
          try {
            JobSynchronizationManager.register(originalExecution);
            delegate.run();
          } finally {
            JobSynchronizationManager.release();
          }
        }
      };
    }
  }

  public static class JobScopeDelegatingCallable {
    protected static <T> Callable<T> create(Callable<T> delegate) {
      var originalExecution = JobSynchronizationManager.getContext().getJobExecution();
      return new Callable() {
        @Override
        public T call() throws Exception {
          try {
            JobSynchronizationManager.register(originalExecution);
            return delegate.call();
          } finally {
            JobSynchronizationManager.release();
          }
        }
      };
    }
  }
}
