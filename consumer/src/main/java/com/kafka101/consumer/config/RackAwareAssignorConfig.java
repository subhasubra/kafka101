package com.kafka101.consumer.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Random;

public class RackAwareAssignorConfig extends AbstractConfig {

    public static final String CONSUMER_RACK_ID = "assignment.consumer.rack";

    private static final ConfigDef CONFIG = new ConfigDef().define(CONSUMER_RACK_ID,
                                                ConfigDef.Type.STRING, "NONE",
                                                ConfigDef.Importance.HIGH, null);

    public RackAwareAssignorConfig(Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public String getRackId() {
        String rackId = getString(CONSUMER_RACK_ID);
        // Only required for test purposes to simulate consumers with
        // different rack configs. Comment out otherwise.
        if (rackId.equals("NONE")) {
            Random randGen = new Random();
            int randInt = randGen.nextInt(4) + 1;
            rackId = "in-east-" + randInt;
        }
        return rackId;
    }
}
