package org.jauntsy.grinder.karma.mapred;

/**
 * User: ebishop
 * Date: 8/23/12
 * Time: 1:25 PM
 */
public class Crc64 {

    private static final long poly = 0xC96C5795D7870F42L;
    private static final long crcTable[] = new long[256];

    private long crc = -1;

    static {
        for (int b = 0; b < crcTable.length; ++b) {
            long r = b;
            for (int i = 0; i < 8; ++i) {
                if ((r & 1) == 1)
                    r = (r >>> 1) ^ poly;
                else
                    r >>>= 1;
            }

            crcTable[b] = r;
        }
    }

    public Crc64() {
    }

    public void update(byte[] buf, int off, int len) {
        crc = update(crc, buf, off, len);
    }

    public void update(byte[] bytes) {
        update(bytes, 0, bytes.length);
    }

    public byte[] getValueAsBytes() {
        return getValueAsBytes(getValue());
    }

    public long getValue() {
        return getValue(crc);
    }

    public void reset() {
        crc = -1;
    }

    private static byte[] getValueAsBytes(long value) {
        byte[] buf = new byte[8];
        for (int i = 0; i < buf.length; ++i)
            buf[i] = (byte) (value >> (i * 8));

        return buf;
    }

    public static long update(long crc, byte[] buf, int off, int len) {
        int end = off + len;

        while (off < end)
            crc = crcTable[(buf[off++] ^ (int) crc) & 0xFF] ^ (crc >>> 8);

        return crc;
    }

    public static long getValue(long crc) {
        return ~crc;
    }

    public static long getCrc(byte[] bytes, int off, int len) {
        return getValue(update(-1, bytes, off, len));
    }

    public static long getCrc(byte[] bytes) {
        return getCrc(bytes, 0, bytes.length);
    }


    public static void main(String[] args) {
        for (int i = 0; i < 22; i++) {
            String s = "" + (char)('a' + i);
            Crc64 crc = new Crc64();
            crc.update(s.getBytes());
            System.out.println(s + " = " + crc.getValue());
        }
        for (int i = 0; i < 22; i++) {
            String s = "" + (char)('a' + i);
            long crc = Crc64.getCrc(s.getBytes());
            System.out.println(s + " = " + crc);
        }
    }

}
