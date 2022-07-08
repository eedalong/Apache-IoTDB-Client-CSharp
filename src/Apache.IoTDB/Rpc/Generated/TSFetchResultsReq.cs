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


public partial class TSFetchResultsReq : TBase
{
  private long _timeout;

  public long SessionId { get; set; }

  public string Statement { get; set; }

  public int FetchSize { get; set; }

  public long QueryId { get; set; }

  public bool IsAlign { get; set; }

  public long Timeout
  {
    get
    {
      return _timeout;
    }
    set
    {
      __isset.timeout = true;
      this._timeout = value;
    }
  }


  public Isset __isset;
  public struct Isset
  {
    public bool timeout;
  }

  public TSFetchResultsReq()
  {
  }

  public TSFetchResultsReq(long sessionId, string statement, int fetchSize, long queryId, bool isAlign) : this()
  {
    this.SessionId = sessionId;
    this.Statement = statement;
    this.FetchSize = fetchSize;
    this.QueryId = queryId;
    this.IsAlign = isAlign;
  }

  public TSFetchResultsReq DeepCopy()
  {
    var tmp91 = new TSFetchResultsReq();
    tmp91.SessionId = this.SessionId;
    if((Statement != null))
    {
      tmp91.Statement = this.Statement;
    }
    tmp91.FetchSize = this.FetchSize;
    tmp91.QueryId = this.QueryId;
    tmp91.IsAlign = this.IsAlign;
    if(__isset.timeout)
    {
      tmp91.Timeout = this.Timeout;
    }
    tmp91.__isset.timeout = this.__isset.timeout;
    return tmp91;
  }

  public async global::System.Threading.Tasks.Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)
  {
    iprot.IncrementRecursionDepth();
    try
    {
      bool isset_sessionId = false;
      bool isset_statement = false;
      bool isset_fetchSize = false;
      bool isset_queryId = false;
      bool isset_isAlign = false;
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
            if (field.Type == TType.I64)
            {
              SessionId = await iprot.ReadI64Async(cancellationToken);
              isset_sessionId = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          case 2:
            if (field.Type == TType.String)
            {
              Statement = await iprot.ReadStringAsync(cancellationToken);
              isset_statement = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          case 3:
            if (field.Type == TType.I32)
            {
              FetchSize = await iprot.ReadI32Async(cancellationToken);
              isset_fetchSize = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          case 4:
            if (field.Type == TType.I64)
            {
              QueryId = await iprot.ReadI64Async(cancellationToken);
              isset_queryId = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          case 5:
            if (field.Type == TType.Bool)
            {
              IsAlign = await iprot.ReadBoolAsync(cancellationToken);
              isset_isAlign = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          case 6:
            if (field.Type == TType.I64)
            {
              Timeout = await iprot.ReadI64Async(cancellationToken);
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
      if (!isset_sessionId)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
      if (!isset_statement)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
      if (!isset_fetchSize)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
      if (!isset_queryId)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
      if (!isset_isAlign)
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
      var struc = new TStruct("TSFetchResultsReq");
      await oprot.WriteStructBeginAsync(struc, cancellationToken);
      var field = new TField();
      field.Name = "sessionId";
      field.Type = TType.I64;
      field.ID = 1;
      await oprot.WriteFieldBeginAsync(field, cancellationToken);
      await oprot.WriteI64Async(SessionId, cancellationToken);
      await oprot.WriteFieldEndAsync(cancellationToken);
      if((Statement != null))
      {
        field.Name = "statement";
        field.Type = TType.String;
        field.ID = 2;
        await oprot.WriteFieldBeginAsync(field, cancellationToken);
        await oprot.WriteStringAsync(Statement, cancellationToken);
        await oprot.WriteFieldEndAsync(cancellationToken);
      }
      field.Name = "fetchSize";
      field.Type = TType.I32;
      field.ID = 3;
      await oprot.WriteFieldBeginAsync(field, cancellationToken);
      await oprot.WriteI32Async(FetchSize, cancellationToken);
      await oprot.WriteFieldEndAsync(cancellationToken);
      field.Name = "queryId";
      field.Type = TType.I64;
      field.ID = 4;
      await oprot.WriteFieldBeginAsync(field, cancellationToken);
      await oprot.WriteI64Async(QueryId, cancellationToken);
      await oprot.WriteFieldEndAsync(cancellationToken);
      field.Name = "isAlign";
      field.Type = TType.Bool;
      field.ID = 5;
      await oprot.WriteFieldBeginAsync(field, cancellationToken);
      await oprot.WriteBoolAsync(IsAlign, cancellationToken);
      await oprot.WriteFieldEndAsync(cancellationToken);
      if(__isset.timeout)
      {
        field.Name = "timeout";
        field.Type = TType.I64;
        field.ID = 6;
        await oprot.WriteFieldBeginAsync(field, cancellationToken);
        await oprot.WriteI64Async(Timeout, cancellationToken);
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
    if (!(that is TSFetchResultsReq other)) return false;
    if (ReferenceEquals(this, other)) return true;
    return System.Object.Equals(SessionId, other.SessionId)
      && System.Object.Equals(Statement, other.Statement)
      && System.Object.Equals(FetchSize, other.FetchSize)
      && System.Object.Equals(QueryId, other.QueryId)
      && System.Object.Equals(IsAlign, other.IsAlign)
      && ((__isset.timeout == other.__isset.timeout) && ((!__isset.timeout) || (System.Object.Equals(Timeout, other.Timeout))));
  }

  public override int GetHashCode() {
    int hashcode = 157;
    unchecked {
      hashcode = (hashcode * 397) + SessionId.GetHashCode();
      if((Statement != null))
      {
        hashcode = (hashcode * 397) + Statement.GetHashCode();
      }
      hashcode = (hashcode * 397) + FetchSize.GetHashCode();
      hashcode = (hashcode * 397) + QueryId.GetHashCode();
      hashcode = (hashcode * 397) + IsAlign.GetHashCode();
      if(__isset.timeout)
      {
        hashcode = (hashcode * 397) + Timeout.GetHashCode();
      }
    }
    return hashcode;
  }

  public override string ToString()
  {
    var sb = new StringBuilder("TSFetchResultsReq(");
    sb.Append(", SessionId: ");
    SessionId.ToString(sb);
    if((Statement != null))
    {
      sb.Append(", Statement: ");
      Statement.ToString(sb);
    }
    sb.Append(", FetchSize: ");
    FetchSize.ToString(sb);
    sb.Append(", QueryId: ");
    QueryId.ToString(sb);
    sb.Append(", IsAlign: ");
    IsAlign.ToString(sb);
    if(__isset.timeout)
    {
      sb.Append(", Timeout: ");
      Timeout.ToString(sb);
    }
    sb.Append(')');
    return sb.ToString();
  }
}
