package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.util.ThreadHints;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

public class BlockingWaitStrategy implements WaitStrategy
{

    private static class ThreadWrapper
    {
        Thread  thread;
        volatile boolean cancel;

        public ThreadWrapper(Thread thread)
        {
            this.thread = thread;
            this.cancel = false;
        }
    }

    private final ConcurrentLinkedQueue<ThreadWrapper> queue = new ConcurrentLinkedQueue<>();

    @Override
    public long waitFor(long sequence,
                        Sequence cursor,
                        Sequence dependentSequence,
                        SequenceBarrier barrier) throws AlertException
    {

        if (cursor.get() < sequence)
        {
            while (cursor.get() < sequence)
            {
                barrier.checkAlert();

                ThreadWrapper wrapper = new ThreadWrapper(Thread.currentThread());
                queue.offer(wrapper);
                if (cursor.get() < sequence)
                {
                    LockSupport.park(this);
                }
                else
                {
                    wrapper.cancel = true;
                    break;
                }
            }
        }

        long availableSequence;
        while ((availableSequence = dependentSequence.get()) < sequence)
        {
            barrier.checkAlert();
            ThreadHints.onSpinWait();
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {
        ThreadWrapper wrapper;
        while ((wrapper = queue.poll()) != null && !wrapper.cancel)
        {
            LockSupport.unpark(wrapper.thread);
        }
    }
}
