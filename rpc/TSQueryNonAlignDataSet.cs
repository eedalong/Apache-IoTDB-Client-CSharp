/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Thrift;
using Thrift.Collections;
using System.Runtime.Serialization;
using Thrift.Protocol;
using Thrift.Transport;


#if !SILVERLIGHT
[Serializable]
#endif
public partial class TSQueryNonAlignDataSet : TBase
{

  public List<byte[]> TimeList { get; set; }

  public List<byte[]> ValueList { get; set; }

  public TSQueryNonAlignDataSet() {
  }

  public TSQueryNonAlignDataSet(List<byte[]> timeList, List<byte[]> valueList) : this() {
    this.TimeList = timeList;
    this.ValueList = valueList;
  }

  public void Read (TProtocol iprot)
  {
    iprot.IncrementRecursionDepth();
    try
    {
      bool isset_timeList = false;
      bool isset_valueList = false;
      TField field;
      iprot.ReadStructBegin();
      while (true)
      {
        field = iprot.ReadFieldBegin();
        if (field.Type == TType.Stop) { 
          break;
        }
        switch (field.ID)
        {
          case 1:
            if (field.Type == TType.List) {
              {
                TimeList = new List<byte[]>();
                TList _list12 = iprot.ReadListBegin();
                for( int _i13 = 0; _i13 < _list12.Count; ++_i13)
                {
                  byte[] _elem14;
                  _elem14 = iprot.ReadBinary();
                  TimeList.Add(_elem14);
                }
                iprot.ReadListEnd();
              }
              isset_timeList = true;
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 2:
            if (field.Type == TType.List) {
              {
                ValueList = new List<byte[]>();
                TList _list15 = iprot.ReadListBegin();
                for( int _i16 = 0; _i16 < _list15.Count; ++_i16)
                {
                  byte[] _elem17;
                  _elem17 = iprot.ReadBinary();
                  ValueList.Add(_elem17);
                }
                iprot.ReadListEnd();
              }
              isset_valueList = true;
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          default: 
            TProtocolUtil.Skip(iprot, field.Type);
            break;
        }
        iprot.ReadFieldEnd();
      }
      iprot.ReadStructEnd();
      if (!isset_timeList)
        throw new TProtocolException(TProtocolException.INVALID_DATA, "required field TimeList not set");
      if (!isset_valueList)
        throw new TProtocolException(TProtocolException.INVALID_DATA, "required field ValueList not set");
    }
    finally
    {
      iprot.DecrementRecursionDepth();
    }
  }

  public void Write(TProtocol oprot) {
    oprot.IncrementRecursionDepth();
    try
    {
      TStruct struc = new TStruct("TSQueryNonAlignDataSet");
      oprot.WriteStructBegin(struc);
      TField field = new TField();
      if (TimeList == null)
        throw new TProtocolException(TProtocolException.INVALID_DATA, "required field TimeList not set");
      field.Name = "timeList";
      field.Type = TType.List;
      field.ID = 1;
      oprot.WriteFieldBegin(field);
      {
        oprot.WriteListBegin(new TList(TType.String, TimeList.Count));
        foreach (byte[] _iter18 in TimeList)
        {
          oprot.WriteBinary(_iter18);
        }
        oprot.WriteListEnd();
      }
      oprot.WriteFieldEnd();
      if (ValueList == null)
        throw new TProtocolException(TProtocolException.INVALID_DATA, "required field ValueList not set");
      field.Name = "valueList";
      field.Type = TType.List;
      field.ID = 2;
      oprot.WriteFieldBegin(field);
      {
        oprot.WriteListBegin(new TList(TType.String, ValueList.Count));
        foreach (byte[] _iter19 in ValueList)
        {
          oprot.WriteBinary(_iter19);
        }
        oprot.WriteListEnd();
      }
      oprot.WriteFieldEnd();
      oprot.WriteFieldStop();
      oprot.WriteStructEnd();
    }
    finally
    {
      oprot.DecrementRecursionDepth();
    }
  }

  public override string ToString() {
    StringBuilder __sb = new StringBuilder("TSQueryNonAlignDataSet(");
    __sb.Append(", TimeList: ");
    __sb.Append(TimeList);
    __sb.Append(", ValueList: ");
    __sb.Append(ValueList);
    __sb.Append(")");
    return __sb.ToString();
  }

}

