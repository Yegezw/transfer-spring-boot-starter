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

    private final ConcurrentLinkedQueue<Thread> queue = new ConcurrentLinkedQueue<>();

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

                Thread thread = Thread.currentThread();
                queue.offer(thread);
                LockSupport.park(this);
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
        Thread thread;
        while ((thread = queue.poll()) != null)
        {
            LockSupport.unpark(thread);
        }
    }
}
