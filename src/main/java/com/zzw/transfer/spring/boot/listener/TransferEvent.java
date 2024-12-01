package com.zzw.transfer.spring.boot.listener;

import java.util.EventObject;

public abstract class TransferEvent extends EventObject
{

    /**
     * Constructs a prototypical Event.
     *
     * @param mark the object on which the Event initially occurred
     * @throws IllegalArgumentException if source is null
     */
    public TransferEvent(Object mark)
    {
        super(mark);
    }

    public Object getMark()
    {
        return super.getSource();
    }
}
