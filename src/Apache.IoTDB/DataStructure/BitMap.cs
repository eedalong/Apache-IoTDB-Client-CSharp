using System;
#if NET461_OR_GREATER || NETSTANDARD2_0
using Grax32.Extensions;
#endif
public class BitMap
{
    private static byte[] BIT_UTIL = new byte[] { 1, 2, 4, 8, 16, 32, 64, 255 };
    private static byte[] UNMARK_BIT_UTIL =
      new byte[] {
        (byte) 0XFE, // 11111110
        (byte) 0XFD, // 11111101
        (byte) 0XFB, // 11111011
        (byte) 0XF7, // 11110111
        (byte) 0XEF, // 11101111
        (byte) 0XDF, // 11011111
        (byte) 0XBF, // 10111111
        (byte) 0X7F // 01111111
      };
    private byte[] bits;
    private int size;

    /** Initialize a BitMap with given size. */
    public BitMap(int size)
    {
        this.size = size;
        bits = new byte[size / 8 + 1];
#if NET461_OR_GREATER || NETSTANDARD2_0
        bits.Fill((byte)0);
#else
        Array.Fill<byte>(bits, (byte)0);
#endif
    }

    /** Initialize a BitMap with given size and bytes. */
    public BitMap(int size, byte[] bits)
    {
        this.size = size;
        this.bits = bits;
    }

    public byte[] getByteArray()
    {
        return this.bits;
    }

    public int getSize()
    {
        return this.size;
    }

    /** returns the value of the bit with the specified index. */
    public bool isMarked(int position)
    {
        return (bits[position / 8] & BIT_UTIL[position % 8]) != 0;
    }

    /** mark as 1 at all positions. */
    public void markAll()
    {
#if NET461_OR_GREATER || NETSTANDARD2_0
        bits.Fill((byte)0xFF);
#else
        Array.Fill(bits, (byte)0xFF);
#endif

    }

    /** mark as 1 at the given bit position. */
    public void mark(int position)
    {
        bits[position / 8] |= BIT_UTIL[position % 8];
    }

    /** mark as 0 at all positions. */
    public void reset()
    {
#if NET461_OR_GREATER || NETSTANDARD2_0
        bits.Fill((byte)0xFF);
#else
        Array.Fill(bits, (byte)0);
#endif
    }

    public void unmark(int position)
    {
        bits[position / 8] &= UNMARK_BIT_UTIL[position % 8];
    }

    /** whether all bits are zero, i.e., no Null value */
    public bool isAllUnmarked()
    {
        int j;
        for (j = 0; j < size / 8; j++)
        {
            if (bits[j] != (byte)0)
            {
                return false;
            }
        }
        for (j = 0; j < size % 8; j++)
        {
            if ((bits[size / 8] & BIT_UTIL[j]) != 0)
            {
                return false;
            }
        }
        return true;
    }

    /** whether all bits are one, i.e., all are Null */
    public bool isAllMarked()
    {
        int j;
        for (j = 0; j < size / 8; j++)
        {
            if (bits[j] != (byte)0XFF)
            {
                return false;
            }
        }
        for (j = 0; j < size % 8; j++)
        {
            if ((bits[size / 8] & BIT_UTIL[j]) == 0)
            {
                return false;
            }
        }
        return true;
    }
}