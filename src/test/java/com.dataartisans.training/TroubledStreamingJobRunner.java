package com.dataartisans.training;

import org.junit.Test;

public class TroubledStreamingJobRunner {

    @Test
    public void run() throws Exception {
        String[] args = {"--test", "true"};
        TroubledStreamingJob.main(args);
    }
}
