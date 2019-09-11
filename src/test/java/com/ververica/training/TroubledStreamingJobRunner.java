package com.ververica.training;

import com.ververica.flinktraining.exercises.troubleshoot.TroubledStreamingJob;
import org.junit.Test;

public class TroubledStreamingJobRunner {

    @Test
    public void run() throws Exception {
        String[] args = {"--local", "true"};
        TroubledStreamingJob.main(args);
    }
}
