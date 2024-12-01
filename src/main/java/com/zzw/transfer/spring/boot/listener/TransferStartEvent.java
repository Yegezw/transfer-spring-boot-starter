package com.zzw.transfer.spring.boot.listener;

public class TransferStartEvent extends TransferEvent
{

    /**
     * Constructs a prototypical Event.
     *
     * @param mark the object on which the Event initially occurred
     * @throws IllegalArgumentException if source is null
     */
    public TransferStartEvent(Object mark)
    {
        super(mark);
    }
}
