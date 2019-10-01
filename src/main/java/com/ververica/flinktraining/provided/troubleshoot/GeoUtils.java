package com.ververica.flinktraining.provided.troubleshoot;

import com.ververica.flinktraining.provided.DoNotChangeThis;

@SuppressWarnings("WeakerAccess")
@DoNotChangeThis
public class GeoUtils {

    // bounding box of the area of the USA
    public final static double US_LON_EAST = -66.9326;
    public final static double US_LON_WEST = -125.0011;
    public final static double US_LAT_NORTH = 49.5904;
    public final static double US_LAT_SOUTH =  24.9493;

    /**
     * Checks if a location specified by longitude and latitude values is
     * within the geo boundaries of the USA.
     *
     * @param lon longitude of the location to check
     * @param lat latitude of the location to check
     *
     * @return true if the location is within US boundaries, otherwise false.
     */
    public static boolean isInUS(double lon, double lat) {
        return !(lon > US_LON_EAST || lon < US_LON_WEST) &&
                !(lat > US_LAT_NORTH || lat < US_LAT_SOUTH);
    }

    // bounding box of the area of the USA
    public final static double DE_LON_EAST = 15.0419319;
    public final static double DE_LON_WEST = 5.8663153;
    public final static double DE_LAT_NORTH = 55.099161;
    public final static double DE_LAT_SOUTH =  47.2701114;

    /**
     * Checks if a location specified by longitude and latitude values is
     * within the geo boundaries of Germany.
     *
     * @param lon longitude of the location to check
     * @param lat latitude of the location to check
     *
     * @return true if the location is within German boundaries, otherwise false.
     */
    public static boolean isInDE(double lon, double lat) {
        return !(lon > DE_LON_EAST || lon < DE_LON_WEST) &&
                !(lat > DE_LAT_NORTH || lat < DE_LAT_SOUTH);
    }
}
