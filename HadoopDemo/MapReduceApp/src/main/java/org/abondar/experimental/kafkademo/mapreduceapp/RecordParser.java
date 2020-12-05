package org.abondar.experimental.kafkademo.mapreduceapp;


import org.apache.hadoop.io.Text;

public class RecordParser {
    private static final int MISSING_TEMP = 9999;
    private String year;
    private int airTemp;
    private String quality;
    private boolean airTempMalformed;

    public void parse(String record) {
        year = record.substring(15, 19);
        String airTempStr;

        airTempMalformed = false;
        if (record.charAt(87) == '+') {
            airTempStr = record.substring(88, 92);
        } else {
            airTempStr = record.substring(87, 92);
        }

        airTemp = Integer.parseInt(airTempStr);
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return !airTempMalformed && airTemp != MISSING_TEMP && quality.matches("[01459]");
    }

    public String getYear() {
        return year;
    }

    public int getAirTemp() {
        return airTemp;
    }

    public boolean isAirTempMalformed() {
        return airTempMalformed;
    }


}
