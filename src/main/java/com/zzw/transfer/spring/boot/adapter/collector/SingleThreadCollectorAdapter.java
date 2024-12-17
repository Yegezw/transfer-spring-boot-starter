package com.zzw.transfer.spring.boot.adapter.collector;

import com.lmax.disruptor.EventHandler;
import com.zzw.transfer.spring.boot.transfer.Bucket;
import com.zzw.transfer.spring.boot.transfer.Transfer;
import com.zzw.transfer.spring.boot.transfer.TransferRepository;

/**
 * 单线程收集器
 */
public class SingleThreadCollectorAdapter implements EventHandler<Bucket>
{

    private final TransferRepository transferRepository;

    public SingleThreadCollectorAdapter(TransferRepository transferRepository)
    {
        this.transferRepository = transferRepository;
    }

    @Override
    public void onEvent(Bucket bucket, long sequence, boolean endOfBatch)
    {
        Object         mark     = bucket.getMark();
        Transfer<?, ?> transfer = transferRepository.get(mark);
        transfer.collect(bucket);
    }
}
