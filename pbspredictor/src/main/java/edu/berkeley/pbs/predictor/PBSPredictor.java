/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.pbs.predictor;

import edu.berkeley.pbs.predictor.PBSPredictionResult;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import edu.berkeley.pbs.trace.PBSLatencyTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs latency and consistency predictions as described in
 * <a href="http://arxiv.org/pdf/1204.6082.pdf">
 * "Probabilistically Bounded Staleness for Practical Partial Quorums"</a>
 * by Bailis et al. in VLDB 2012. The predictions are of the form:
 * <p/>
 * <i>With ReplicationFactor <tt>N</tt>, read consistency level of
 * <tt>R</tt>, and write consistency level <tt>W</tt>, after
 * <tt>t</tt> seconds, <tt>p</tt>% of reads will return a version
 * within <tt>k</tt> versions of the last written; this should result
 * in a latency of <tt>L</tt> ms.</i>
 * <p/>
 * <p/>
 * These predictions should be used as a rough guideline for system
 * operators. This interface is exposed through nodetool.
 * <p/>
 * <p/>
 * The class accomplishes this by measuring latencies for reads and
 * writes, then using Monte Carlo simulation to predict behavior under
 * a given N,R, and W based on those latencies.
 * <p/>
 * <p/>
 * We capture four distributions:
 * <p/>
 * <ul>
 * <li>
 * <tt>W</tt>: time from when the coordinator sends a mutation to the time
 * that a replica begins to serve the new value(s)
 * </li>
 * <p/>
 * <li>
 * <tt>A</tt>: time from when a replica accepting a mutation sends an
 * acknowledgment to the time the coordinator hears of it
 * </li>
 * <p/>
 * <li>
 * <tt>R</tt>: time from when the coordinator sends a read request to the time
 * that the replica performs the read
 * </li>
 * <p/>
 * <li>
 * <tt>S</tt>: time from when the replica sends a read response to the time
 * when the coordinator receives it
 * </li>
 * </ul>
 * <p/>
 * <tt>A</tt> and <tt>S</tt> are mostly network-bound, while W and R
 * depend on both the network and local processing time.
 * <p/>
 * <p/>
 * <b>Caveats:</b>
 * Prediction is only as good as the latencies collected. Accurate
 * prediction requires synchronizing clocks between replicas.  We
 * collect a running sample of latencies, but, if latencies change
 * dramatically, predictions will be off.
 * <p/>
 * <p/>
 * The predictions are conservative, or worst-case, meaning we may
 * predict more staleness than in practice in the following ways:
 * <ul>
 * <li>
 * We do not account for read repair.
 * </li>
 * <li>
 * We do not account for Merkle tree exchange.
 * </li>
 * <li>
 * Multi-version staleness is particularly conservative.
 * </li>
 * <li>
 * We simulate non-local reads and writes. We assume that the
 * coordinating Cassandra node is not itself a replica for a given key.
 * </li>
 * </ul>
 * <p/>
 * <p/>
 * The predictions are optimistic in the following ways:
 * <ul>
 * <li>
 * We do not predict the impact of node failure.
 * </li>
 * <li>
 * We do not model hinted handoff.
 * </li>
 * </ul>
 */

public class PBSPredictor
{
    // used for calculating the average latency of a read or write operation
    // given a set of simulated latencies
    private static double listAverage(List<Double> list)
    {
        long accum = 0;
        for (double value : list)
            accum += value;
        return (float) accum / list.size();
    }

    // calculate the percentile entry of a list
    private static double getPercentile(List<Double> list, double percentile)
    {
        Collections.sort(list);
        return list.get((int) (list.size() * percentile));
    }

    /*
     *  To perform the prediction, we randomly sample from the
     *  collected WARS latencies, simulating writes followed by reads
     *  exactly t milliseconds afterwards. We count the number of
     *  reordered reads and writes to calculate the probability of
     *  staleness along with recording operation latencies.
     */


    public static PBSPredictionResult doPrediction(int n,
                                                   int r,
                                                   int w,
                                                   double timeSinceWrite,
                                                   int numberVersionsStale,
                                                   double percentileLatency,
                                                   int numberTrialsPrediction,
                                                   PBSLatencyTrace trace)
    {
        if (r > n)
            throw new IllegalArgumentException("r must be less than n");
        if (r < 0)
            throw new IllegalArgumentException("r must be positive");
        if (w > n)
            throw new IllegalArgumentException("w must be less than n");
        if (w < 0)
            throw new IllegalArgumentException("w must be positive");
        if(numberVersionsStale < 1)
            throw new IllegalArgumentException("numberVersionsStale must be greater than one");
        if (percentileLatency < 0 || percentileLatency > 1)
            throw new IllegalArgumentException("percentileLatency must be between 0 and 1 inclusive");
        if (numberVersionsStale < 0)
            throw new IllegalArgumentException("numberVersionsStale must be positive");


        // storage for simulated read and write latencies
        ArrayList<Double> readLatencies = new ArrayList<Double>();
        ArrayList<Double> writeLatencies = new ArrayList<Double>();

        long consistentReads = 0;

        // storage for latencies for each replica for a given Monte Carlo trial
        // arr[i] will hold the ith replica's latency for one of WARS
        ArrayList<Double> trialWLatencies = new ArrayList<Double>();
        ArrayList<Double> trialRLatencies = new ArrayList<Double>();

        ArrayList<Double> replicaWriteLatencies = new ArrayList<Double>();
        ArrayList<Double> replicaReadLatencies = new ArrayList<Double>();

        //run repeated trials and observe staleness
        for (int i = 0; i < numberTrialsPrediction; ++i)
        {
            //simulate sending a write to N replicas then sending a
            //read to N replicas and record the latencies by randomly
            //sampling from gathered latencies
            for (int replicaNo = 0; replicaNo < n; ++replicaNo)
            {
                double trialWLatency = trace.getNextWValue();
                double trialALatency = trace.getNextAValue();

                trialWLatencies.add(trialWLatency);

                replicaWriteLatencies.add(trialWLatency + trialALatency);
            }

            // reads are only sent to R replicas - so pick R random read and
            // response latencies
            for (int replicaNo = 0; replicaNo < r; ++replicaNo)
            {
                double trialRLatency = trace.getNextRValue();
                double trialSLatency = trace.getNextWValue();

                trialRLatencies.add(trialRLatency);

                replicaReadLatencies.add(trialRLatency + trialSLatency);
            }

            // the write latency for this trial is the time it takes
            // for the wth replica to respond (W+A)
            Collections.sort(replicaWriteLatencies);
            double writeLatency = replicaWriteLatencies.get(w - 1);
            writeLatencies.add(writeLatency);

            ArrayList<Double> sortedReplicaReadLatencies = new ArrayList<Double>(replicaReadLatencies);
            Collections.sort(sortedReplicaReadLatencies);

            // the read latency for this trial is the time it takes
            // for the rth replica to respond (R+S)
            readLatencies.add(sortedReplicaReadLatencies.get(r - 1));

            // were all of the read responses reordered?

            // for each of the first r messages (the ones the
            // coordinator will pick from):
            //--if the read message came in after this replica saw the
            // write, it will be consistent
            //--each read request is sent at time
            // writeLatency+timeSinceWrite

            for (int responseNumber = 0; responseNumber < r; ++responseNumber)
            {
                int replicaNumber = replicaReadLatencies.indexOf(sortedReplicaReadLatencies.get(responseNumber));

                if (writeLatency + timeSinceWrite + trialRLatencies.get(replicaNumber) >=
                    trialWLatencies.get(replicaNumber))
                {
                    consistentReads++;
                    break;
                }

                // tombstone this replica in the case that we have
                // duplicate read latencies
                replicaReadLatencies.set(replicaNumber, -1D);
            }

            // clear storage for the next trial
            trialWLatencies.clear();
            trialRLatencies.clear();

            replicaReadLatencies.clear();
            replicaWriteLatencies.clear();
        }

        float oneVersionConsistencyProbability = (float) consistentReads / numberTrialsPrediction;

        // to calculate multi-version staleness, we exponentiate the staleness probability by the number of versions
        float consistencyProbability = (float) (1 - Math.pow((double) (1 - oneVersionConsistencyProbability),
                                                             numberVersionsStale));

        double averageWriteLatency = listAverage(writeLatencies);
        double averageReadLatency = listAverage(readLatencies);

        double percentileWriteLatency = getPercentile(writeLatencies, percentileLatency);
        double percentileReadLatency = getPercentile(readLatencies, percentileLatency);

        return new PBSPredictionResult(n,
                                       r,
                                       w,
                                       timeSinceWrite,
                                       numberVersionsStale,
                                       consistencyProbability,
                                       averageReadLatency,
                                       averageWriteLatency,
                                       percentileReadLatency,
                                       percentileLatency,
                                       percentileWriteLatency,
                                       percentileLatency);
    }
}