package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.WorkHandler;

/**
 * 多线程消费者
 */
@SuppressWarnings("all")
public class MultiHandlerWrapper implements WorkHandler<Bucket>
{

    private final Dispatcher dispatcher;

    public MultiHandlerWrapper(Dispatcher dispatcher)
    {
        this.dispatcher = dispatcher;
    }

    @Override
    public void onEvent(Bucket bucket)
    {
        Object   mark     = bucket.getMark();
        Transfer transfer = dispatcher.getTransfer(mark);
        transfer.handle(bucket);
    }
}
