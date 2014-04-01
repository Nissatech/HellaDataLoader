package com.nissatech.helladataloader;

/**
 *
 * @author aleksandar
 */
public class Rounder60min implements ITimeRounder
{

    private final int roundingFactor;
    public Rounder60min()
    {
        this.roundingFactor = 1000 * 60 * 60;
    }
    
    public long round(long time)
    {
        return (time/(roundingFactor))*roundingFactor;
    }
    
}
