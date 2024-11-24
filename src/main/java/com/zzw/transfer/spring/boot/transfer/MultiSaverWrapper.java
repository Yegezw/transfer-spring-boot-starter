package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.WorkHandler;

/**
 * 多线程保存者
 */
@SuppressWarnings("all")
public class MultiSaverWrapper implements WorkHandler<Bucket>
{

    private final Dispatcher dispatcher;

    public MultiSaverWrapper(Dispatcher dispatcher)
    {
        this.dispatcher = dispatcher;
    }

    @Override
    public void onEvent(Bucket bucket)
    {
        Object   mark     = bucket.getMark();
        Transfer transfer = dispatcher.getTransfer(mark);
        transfer.save(bucket);
    }
}
