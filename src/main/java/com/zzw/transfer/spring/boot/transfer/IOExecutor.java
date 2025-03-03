package com.zzw.transfer.spring.boot.transfer;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class IOExecutor
{

    private static final String THREAD_NAME_PREFIX = "IOExecutor-";
    private static final AtomicInteger COUNT = new AtomicInteger(0);
    private static final int NCPU = Runtime.getRuntime().availableProcessors();
    private static final RejectedExecutionHandler defaultHandler = new ThreadPoolExecutor.AbortPolicy();
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            NCPU, NCPU * 4,
            600, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(32),
            r ->
            {
                Thread thread = new Thread(r);
                thread.setName(THREAD_NAME_PREFIX + COUNT.addAndGet(1));
                return thread;
            },
            defaultHandler
    );

    private IOExecutor()
    {
    }

    public <V> CompletableFuture<V> submit(Supplier<V> supplier)
    {
        return CompletableFuture.supplyAsync(supplier, executor);
    }

    public static void waitAll(CompletableFuture<?>... cfs)
    {
        CompletableFuture.allOf(cfs).join();
    }
}
