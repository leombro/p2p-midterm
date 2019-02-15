package it.unipi.di.p2p;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.math.BigInteger;

/**
 * Class that contains utility methods used in the application.
 */
public class Util {

    /**
     * String used to convert byte arrays into hex strings
     */
    private final static char[] hexArray = "0123456789abcdef".toCharArray();

    /**
     * Converts a byte array into an hex string representation.
     *
     * This method removes all leading zeroes.
     *
     * @param bytes the array to be converted.
     * @return a String containing the byte representation in hexadecimal form.
     */
    public static String bytesToHex(byte[] bytes) {
        return bytesToHex_NoTrim(bytes).replaceFirst("^0+(?!$)", "");
    }

    /**
     * Converts a byte array into an hex string representation.
     *
     * This method does not remove any leading zeroes.
     *
     * @param bytes the array to be converted.
     * @return a String containing the byte representation in hexadecimal form.
     */
    public static String bytesToHex_NoTrim(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * Checks whether a BigInteger is inside an interval, which can belong to a ring (and so
     * wrap around a wrapPoint).
     *
     * @param rightBounded a boolean to indicate whether the interval is right-closed
     * @param key the BigInteger to be checked against the interval
     * @param leftEndpoint the left endpoint of the interval
     * @param rightEndpoint the right endpoint of the interval
     * @param wrapPoint the point where the whole ring wraps around
     * @return true if key is in the interval delimited by leftEndpoint and rightEndpoint
     */
    public static boolean isInInterval(boolean rightBounded,
                                       BigInteger key,
                                       BigInteger leftEndpoint,
                                       BigInteger rightEndpoint,
                                       BigInteger wrapPoint) {
        if (rightEndpoint.compareTo(leftEndpoint) < 0) {
            // If the right endpoint is lower than the left one, the ring is wrapping inside the interval
            boolean inLeftPortion = key.compareTo(leftEndpoint) > 0 && key.compareTo(wrapPoint) < 0;
            return inLeftPortion
                    || // right portion; written in this way to allow short-circuit lazy evaluation
                    key.compareTo(BigInteger.ZERO) >= 0
                            && (rightBounded ? key.compareTo(rightEndpoint) <= 0 : key.compareTo(rightEndpoint) < 0);
        } else {
            return (key.compareTo(leftEndpoint) > 0
                    && (rightBounded ? key.compareTo(rightEndpoint) <= 0 : key.compareTo(rightEndpoint) < 0));
        }
    }

    /**
     * Truncates a byte array to a set size.
     *
     * The byte array gets first converted into a hex string, which is truncated and
     * returned back as a byte array using the Apache Commons library.
     *
     * @param source The byte array to be truncated
     * @param to_bits The size (multiple of 4) to truncate
     * @return The truncated byte array
     */
    public static byte[] truncate(byte[] source, int to_bits) {
        String from = bytesToHex_NoTrim(source);
        int nibbles = (int) Math.ceil((double) to_bits / 4);
        if (nibbles >= from.length()) return source;
        else {
            try {
                String s = from.substring(0, nibbles);
                if (s.length() % 2 != 0) {
                    s = "0" + s;
                }
                return Hex.decodeHex(s);
            } catch (DecoderException e) {
                throw new RuntimeException("Could not truncate");
            }
        }
    }
}
