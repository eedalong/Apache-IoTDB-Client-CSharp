/**
 * Autogenerated by Thrift Compiler (0.14.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Thrift;
using Thrift.Collections;

using Thrift.Protocol;
using Thrift.Protocol.Entities;
using Thrift.Protocol.Utilities;
using Thrift.Transport;
using Thrift.Transport.Client;
using Thrift.Transport.Server;
using Thrift.Processor;


#pragma warning disable IDE0079  // remove unnecessary pragmas
#pragma warning disable IDE1006  // parts of the code use IDL spelling


public partial class TSConnectionInfoResp : TBase
{

  public List<TSConnectionInfo> ConnectionInfoList { get; set; }

  public TSConnectionInfoResp()
  {
  }

  public TSConnectionInfoResp(List<TSConnectionInfo> connectionInfoList) : this()
  {
    this.ConnectionInfoList = connectionInfoList;
  }

  public TSConnectionInfoResp DeepCopy()
  {
    var tmp429 = new TSConnectionInfoResp();
    if((ConnectionInfoList != null))
    {
      tmp429.ConnectionInfoList = this.ConnectionInfoList.DeepCopy();
    }
    return tmp429;
  }

  public async global::System.Threading.Tasks.Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)
  {
    iprot.IncrementRecursionDepth();
    try
    {
      bool isset_connectionInfoList = false;
      TField field;
      await iprot.ReadStructBeginAsync(cancellationToken);
      while (true)
      {
        field = await iprot.ReadFieldBeginAsync(cancellationToken);
        if (field.Type == TType.Stop)
        {
          break;
        }

        switch (field.ID)
        {
          case 1:
            if (field.Type == TType.List)
            {
              {
                TList _list430 = await iprot.ReadListBeginAsync(cancellationToken);
                ConnectionInfoList = new List<TSConnectionInfo>(_list430.Count);
                for(int _i431 = 0; _i431 < _list430.Count; ++_i431)
                {
                  TSConnectionInfo _elem432;
                  _elem432 = new TSConnectionInfo();
                  await _elem432.ReadAsync(iprot, cancellationToken);
                  ConnectionInfoList.Add(_elem432);
                }
                await iprot.ReadListEndAsync(cancellationToken);
              }
              isset_connectionInfoList = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          default: 
            await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            break;
        }

        await iprot.ReadFieldEndAsync(cancellationToken);
      }

      await iprot.ReadStructEndAsync(cancellationToken);
      if (!isset_connectionInfoList)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
    }
    finally
    {
      iprot.DecrementRecursionDepth();
    }
  }

  public async global::System.Threading.Tasks.Task WriteAsync(TProtocol oprot, CancellationToken cancellationToken)
  {
    oprot.IncrementRecursionDepth();
    try
    {
      var struc = new TStruct("TSConnectionInfoResp");
      await oprot.WriteStructBeginAsync(struc, cancellationToken);
      var field = new TField();
      if((ConnectionInfoList != null))
      {
        field.Name = "connectionInfoList";
        field.Type = TType.List;
        field.ID = 1;
        await oprot.WriteFieldBeginAsync(field, cancellationToken);
        {
          await oprot.WriteListBeginAsync(new TList(TType.Struct, ConnectionInfoList.Count), cancellationToken);
          foreach (TSConnectionInfo _iter433 in ConnectionInfoList)
          {
            await _iter433.WriteAsync(oprot, cancellationToken);
          }
          await oprot.WriteListEndAsync(cancellationToken);
        }
        await oprot.WriteFieldEndAsync(cancellationToken);
      }
      await oprot.WriteFieldStopAsync(cancellationToken);
      await oprot.WriteStructEndAsync(cancellationToken);
    }
    finally
    {
      oprot.DecrementRecursionDepth();
    }
  }

  public override bool Equals(object that)
  {
    if (!(that is TSConnectionInfoResp other)) return false;
    if (ReferenceEquals(this, other)) return true;
    return TCollections.Equals(ConnectionInfoList, other.ConnectionInfoList);
  }

  public override int GetHashCode() {
    int hashcode = 157;
    unchecked {
      if((ConnectionInfoList != null))
      {
        hashcode = (hashcode * 397) + TCollections.GetHashCode(ConnectionInfoList);
      }
    }
    return hashcode;
  }

  public override string ToString()
  {
    var sb = new StringBuilder("TSConnectionInfoResp(");
    if((ConnectionInfoList != null))
    {
      sb.Append(", ConnectionInfoList: ");
      ConnectionInfoList.ToString(sb);
    }
    sb.Append(')');
    return sb.ToString();
  }
}
