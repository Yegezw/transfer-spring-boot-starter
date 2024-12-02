package com.zzw.transfer.spring.boot.adapter.handler;

import com.lmax.disruptor.WorkHandler;
import com.zzw.transfer.spring.boot.transfer.Bucket;
import com.zzw.transfer.spring.boot.transfer.TransferRepository;
import com.zzw.transfer.spring.boot.transfer.Transfer;

/**
 * 多线程处理器
 */
public class MultiThreadHandlerAdapter implements WorkHandler<Bucket>
{

    private final TransferRepository transferRepository;

    public MultiThreadHandlerAdapter(TransferRepository transferRepository)
    {
        this.transferRepository = transferRepository;
    }

    @Override
    public void onEvent(Bucket bucket)
    {
        Object         mark     = bucket.getMark();
        Transfer<?, ?> transfer = transferRepository.get(mark);
        transfer.handle(bucket);
    }
}
