package com.zzw.transfer.spring.boot.listener;

public class TransferStopEvent extends TransferEvent
{

    /**
     * Constructs a prototypical Event.
     *
     * @param mark the object on which the Event initially occurred
     * @throws IllegalArgumentException if source is null
     */
    public TransferStopEvent(Object mark)
    {
        super(mark);
    }
}
