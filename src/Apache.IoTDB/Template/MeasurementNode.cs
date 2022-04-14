using System.IO;
using Apache.IoTDB;
using Apache.IoTDB.DataStructure;

namespace Apache.IoTDB
{
    public class MeasurementNode : TemplateNode
    {
        private TSDataType dataType;
        private TSEncoding encoding;
        private Compressor compressor;
        public MeasurementNode(string name, TSDataType dataType, TSEncoding encoding, Compressor compressor) : base(name)
        {
            this.dataType = dataType;
            this.encoding = encoding;
            this.compressor = compressor;
        }
        public override bool isMeasurement()
        {
            return true;
        }
        public TSDataType DataType
        {
            get
            {
                return dataType;
            }
        }
        public TSEncoding Encoding
        {
            get
            {
                return encoding;
            }
        }
        public Compressor Compressor
        {
            get
            {
                return compressor;
            }
        }

        public override byte[] ToBytes()
        {
            var buffer = new ByteBuffer();
            buffer.AddStr(this.Name);
            buffer.AddByte((byte)this.DataType);
            buffer.AddByte((byte)this.Encoding);
            buffer.AddByte((byte)this.Compressor);
            return buffer.GetBuffer();
        }



    }
}