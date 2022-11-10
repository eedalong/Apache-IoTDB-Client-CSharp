using System;
using System.Linq;
using System.Text;

namespace Apache.IoTDB.DataStructure
{
    public class ByteBuffer
    {
        private byte[] _buffer;
        private int _writePos;
        private int _readPos;
        private int _totalLength;
        private readonly bool _isLittleEndian = BitConverter.IsLittleEndian;

        public ByteBuffer(byte[] buffer)
        {
            _buffer = buffer;
            _readPos = 0;
            _writePos = buffer.Length;
            _totalLength = buffer.Length;
        }

        public ByteBuffer(int reserve = 1)
        {
            _buffer = new byte[reserve];
            _writePos = 0;
            _readPos = 0;
            _totalLength = reserve;
        }

        public bool HasRemaining()
        {
            return _readPos < _writePos;
        }

        // these for read
        public byte GetByte()
        {
            var byteVal = _buffer[_readPos];
            _readPos += 1;
            return byteVal;
        }

        public bool GetBool()
        {
            var boolValue = BitConverter.ToBoolean(_buffer, _readPos);
            _readPos += 1;
            return boolValue;
        }

        public int GetInt()
        {
            var intBuff = _buffer[_readPos..(_readPos + 4)];
            if (_isLittleEndian) intBuff = intBuff.Reverse().ToArray();
#if NET461_OR_GREATER || NETSTANDARD2_0
            var intValue = BitConverter.ToInt32(intBuff,0);
#else
            var intValue = BitConverter.ToInt32(intBuff);
#endif

            _readPos += 4;
            return intValue;
        }

        public long GetLong()
        {
            var longBuff = _buffer[_readPos..(_readPos + 8)];

            if (_isLittleEndian) longBuff = longBuff.Reverse().ToArray();
#if NET461_OR_GREATER || NETSTANDARD2_0
            var longValue = BitConverter.ToInt64(longBuff,0);
#else
            var longValue = BitConverter.ToInt64(longBuff);
#endif

            _readPos += 8;
            return longValue;
        }

        public float GetFloat()
        {
            var floatBuff = _buffer[_readPos..(_readPos + 4)];

            if (_isLittleEndian) floatBuff = floatBuff.Reverse().ToArray();
#if NET461_OR_GREATER || NETSTANDARD2_0
            var floatValue = BitConverter.ToSingle(floatBuff,0);
#else
            var floatValue = BitConverter.ToSingle(floatBuff);
#endif
            _readPos += 4;
            return floatValue;
        }

        public double GetDouble()
        {
            var doubleBuff = _buffer[_readPos..(_readPos + 8)];

            if (_isLittleEndian) doubleBuff = doubleBuff.Reverse().ToArray();
#if NET461_OR_GREATER || NETSTANDARD2_0
            var doubleValue = BitConverter.ToDouble(doubleBuff,0);
#else
            var doubleValue = BitConverter.ToDouble(doubleBuff);
#endif
            _readPos += 8;
            return doubleValue;
        }

        public string GetStr()
        {
            var length = GetInt();
            var strBuff = _buffer[_readPos..(_readPos + length)];
            var strValue = Encoding.UTF8.GetString(strBuff);
            _readPos += length;
            return strValue;
        }

        public byte[] GetBuffer()
        {
            return _buffer[.._writePos];
        }

        private void ExtendBuffer(int spaceNeed)
        {
            if (_writePos + spaceNeed >= _totalLength)
            {
                _totalLength = Math.Max(spaceNeed, _totalLength);
                var newBuffer = new byte[_totalLength * 2];
                _buffer.CopyTo(newBuffer, 0);
                _buffer = newBuffer;
                _totalLength = 2 * _totalLength;
            }
        }

        // these for write
        public void AddBool(bool value)
        {
            var boolBuffer = BitConverter.GetBytes(value);

            if (_isLittleEndian) boolBuffer = boolBuffer.Reverse().ToArray();

            ExtendBuffer(boolBuffer.Length);
            boolBuffer.CopyTo(_buffer, _writePos);
            _writePos += boolBuffer.Length;
        }

        public void AddInt(int value)
        {
            var intBuff = BitConverter.GetBytes(value);

            if (_isLittleEndian) intBuff = intBuff.Reverse().ToArray();

            ExtendBuffer(intBuff.Length);
            intBuff.CopyTo(_buffer, _writePos);
            _writePos += intBuff.Length;
        }

        public void AddLong(long value)
        {
            var longBuff = BitConverter.GetBytes(value);

            if (_isLittleEndian) longBuff = longBuff.Reverse().ToArray();

            ExtendBuffer(longBuff.Length);
            longBuff.CopyTo(_buffer, _writePos);
            _writePos += longBuff.Length;
        }

        public void AddFloat(float value)
        {
            var floatBuff = BitConverter.GetBytes(value);

            if (_isLittleEndian) floatBuff = floatBuff.Reverse().ToArray();

            ExtendBuffer(floatBuff.Length);
            floatBuff.CopyTo(_buffer, _writePos);
            _writePos += floatBuff.Length;
        }

        public void AddDouble(double value)
        {
            var doubleBuff = BitConverter.GetBytes(value);

            if (_isLittleEndian) doubleBuff = doubleBuff.Reverse().ToArray();

            ExtendBuffer(doubleBuff.Length);
            doubleBuff.CopyTo(_buffer, _writePos);
            _writePos += doubleBuff.Length;
        }

        public void AddStr(string value)
        {
            var strBuf = Encoding.UTF8.GetBytes(value);

            AddInt(strBuf.Length);

            ExtendBuffer(strBuf.Length);
            strBuf.CopyTo(_buffer, _writePos);
            _writePos += strBuf.Length;
        }

        public void AddChar(char value)
        {
            var charBuf = BitConverter.GetBytes(value);

            if (_isLittleEndian) charBuf = charBuf.Reverse().ToArray();

            ExtendBuffer(charBuf.Length);
            charBuf.CopyTo(_buffer, _writePos);
            _writePos += charBuf.Length;
        }
        public void AddByte(byte value)
        {
            ExtendBuffer(1);
            _buffer[_writePos] = value;
            _writePos += 1;
        }
    }
}