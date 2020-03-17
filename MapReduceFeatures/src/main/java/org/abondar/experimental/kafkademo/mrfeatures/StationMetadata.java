package org.abondar.experimental.kafkademo.mrfeatures;


import org.apache.hadoop.io.IOUtils;

import java.io.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class StationMetadata {
    private Map<String, String> stationIdToName = new HashMap<String, String>();

    public void initialize(File file) throws IOException {
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
           StationMetadataParser parser = new StationMetadataParser();
            String line;
            while ((line = in.readLine()) != null) {
                if (parser.parse(line)) {
                    stationIdToName.put(parser.getStationId(), parser.getStationName());
                }
            }
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public String getStationName(String stationId) {
        String stationName = stationIdToName.get(stationId);
        if (stationName == null || stationName.trim().length() == 0) {
            return stationId; // no match: fall back to ID
        }
        return stationName;
    }

    public Map<String, String> getStationIdToNameMap() {
        return Collections.unmodifiableMap(stationIdToName);
    }
}