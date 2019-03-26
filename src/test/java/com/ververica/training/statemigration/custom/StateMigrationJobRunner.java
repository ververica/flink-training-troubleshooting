package com.ververica.training.statemigration.custom;

import org.junit.Test;

public class StateMigrationJobRunner {

    @Test
    public void run() throws Exception {
        String[] args = {"--local", "true"};
        StateMigrationJob.main(args);
    }
}
