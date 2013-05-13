package edu.berkeley.pbs.trace;

public interface PBSLatencyTrace {
    public double getNextWValue();
    public double getNextAValue();
    public double getNextRValue();
    public double getNextSValue();
}