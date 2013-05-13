
package edu.berkeley.pbs;

import edu.berkeley.pbs.cassandra.CassandraLatencyTrace;
import edu.berkeley.pbs.predictor.PBSPredictor;
import edu.berkeley.pbs.predictor.PBSPredictionResult;

import java.io.PrintStream;

public class PBSPredictorCli {

    public static void main(String[] args) {
        PrintStream output = System.out;
        int replicationFactor = 3;
        int timeAfterWriteMs = 1;
        int numVersions = 1;

        try {
            CassandraLatencyTrace trace = new CassandraLatencyTrace(args[0], Integer.parseInt(args[1]), "Keyspace1", "Standard1");
            for(int r = 1; r <= replicationFactor; ++r) {
                for(int w = 1; w <= replicationFactor; ++w) {
                    if(w+r > replicationFactor+1)
                        continue;

                    PBSPredictionResult result = PBSPredictor.doPrediction(replicationFactor,
                                                                           r,
                                                                           w,
                                                                           1000*timeAfterWriteMs,
                                                                           numVersions,
                                                                           .99,
                                                                           10000,
                                                                           trace);

                    if(r == 1 && w == 1) {
                        output.printf("%dms after a given write, with maximum version staleness of k=%d\n", timeAfterWriteMs, numVersions);
                    }

                    output.printf("N=%d, R=%d, W=%d\n", replicationFactor, r, w);
                    output.printf("Probability of consistent reads: %f\n", result.getConsistencyProbability());
                    output.printf("Average read latency: %fms (%.3fth %%ile %.3fms)\n", result.getAverageReadLatency()/1000,
                                                                                   result.getPercentileReadLatencyPercentile()*100,
                                                                                   result.getPercentileReadLatencyValue()/1000);
                    output.printf("Average write latency: %fms (%.3fth %%ile %.3fms)\n\n", result.getAverageWriteLatency()/1000,
                                                                                      result.getPercentileWriteLatencyPercentile()*100,
                                                                                      result.getPercentileWriteLatencyValue()/1000);

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
