package org.abondar.experimental.kafkademo.mrfeatures;


import org.apache.hadoop.io.Text;

public class RecordParser {
    private static final int MISSING_TEMP = 9999;
    private String year;
    private int airTemp;
    private String quality;
    private boolean airTempMalformed;
    private boolean temperatureMalformed;

    private String stationId;



    public void parse(String record) {

        stationId = record.substring(4,10) + "-" + record.substring(10,15);
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

    public Integer getYearInt() {
        return Integer.valueOf(year);
    }

    public int getAirTemp() {
        return airTemp;
    }

    public boolean isAirTempMalformed() {
        return airTempMalformed;
    }

    public String getStationId() {
        return stationId;
    }

    public boolean isTemperatureMalformed() {
        return temperatureMalformed;
    }

    public void setTemperatureMalformed(boolean temperatureMalformed) {
        this.temperatureMalformed = temperatureMalformed;
    }

    public boolean isMissingTemperature() {
        return airTemp == MISSING_TEMP;
    }

    public String getQuality() {
        return quality;
    }


}
