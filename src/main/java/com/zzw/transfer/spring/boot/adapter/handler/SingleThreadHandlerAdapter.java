package com.zzw.transfer.spring.boot.adapter.handler;

import com.lmax.disruptor.EventHandler;
import com.zzw.transfer.spring.boot.transfer.Bucket;
import com.zzw.transfer.spring.boot.transfer.TransferRepository;
import com.zzw.transfer.spring.boot.transfer.Transfer;

/**
 * 单线程处理器
 */
public class SingleThreadHandlerAdapter implements EventHandler<Bucket>
{

    private final TransferRepository transferRepository;

    public SingleThreadHandlerAdapter(TransferRepository transferRepository)
    {
        this.transferRepository = transferRepository;
    }

    @Override
    public void onEvent(Bucket bucket, long sequence, boolean endOfBatch)
    {
        Object         mark     = bucket.getMark();
        Transfer<?, ?> transfer = transferRepository.get(mark);
        transfer.handle(bucket);
    }
}
