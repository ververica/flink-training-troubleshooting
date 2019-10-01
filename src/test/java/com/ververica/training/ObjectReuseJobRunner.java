package com.ververica.training;

import com.ververica.flinktraining.exercises.troubleshoot.ObjectReuseJob;
import org.junit.Test;

public class ObjectReuseJobRunner {

    @Test
    public void run() throws Exception {
        String[] args = {"--local", "true", "--objectReuse", "true"};
//        String[] args = {"--local", "true"};
        ObjectReuseJob.main(args);
    }
}
