package com.ververica.training.statemigration.custom;

import com.ververica.training.DoNotChangeThis;
import com.ververica.training.statemigration.StateMigrationJobBase;

/**
 * State migration job for custom serializer state migration / schema evolution.
 */
@DoNotChangeThis
public class StateMigrationJob extends StateMigrationJobBase {

	public static void main(String[] args) throws Exception {
		createAndExecuteJob(args, new SensorAggregationProcessing());
	}
}
