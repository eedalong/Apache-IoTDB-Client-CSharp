using Thrift;
using Thrift.Transport;
using Thrift.Protocol;
using Thrift.Server;
using System;
using System.Linq;
using System.Net.Sockets;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Thrift.Collections;


using Thrift.Protocol.Entities;
using Thrift.Protocol.Utilities;
using Thrift.Transport.Client;
using Thrift.Transport.Server;
using Thrift.Processor;

namespace iotdb_client_csharp.client
{
    public class Session
    {
       private string username="root", password="root", zoneId, host;
       private int port, fetch_size=10000;
       private long sessionId, statementId;
       private bool is_close = true;

       private TSIService.Client client; 
       private TSocketTransport transport;
       private static readonly TConfiguration configuration = null;  // new TConfiguration() if  needed
       private static TSProtocolVersion protocol_version = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;


       public Session(string host, int port){
           // init success code 
           this.host = host;
           this.port = port;
       } 
        public Session(string host, int port, string username="root", string password="root", int fetch_size=10000, string zoneId = "UTC+08:00"){
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.zoneId = zoneId;
            this.fetch_size = fetch_size;
        }
        public void open(bool enableRPCCompression){
            if(!is_close){
                return ;
            }
            this.transport = new TSocketTransport(this.host, this.port, configuration);
            if(!transport.IsOpen){
                try{
                    var task = transport.OpenAsync(new CancellationToken());
                    task.Wait();
                }
                catch(TTransportException e){
                    //TODO, should define our own Exception
                    // here we just print the exception
                    Console.Write(e.ToString());
                }
            }
            if(enableRPCCompression){
                client = new TSIService.Client(new TCompactProtocol(transport));
            }else{
                client = new TSIService.Client(new TBinaryProtocol(transport));
            }
            // init open request
            var open_req = new TSOpenSessionReq(protocol_version, zoneId);
            open_req.Username = username;
            open_req.Password = password;
            try{
                var task = client.openSessionAsync(open_req);
                task.Wait();
                var open_resp = task.Result;
                if(open_resp.ServerProtocolVersion != protocol_version){
                    var message = String.Format("Protocol Differ, Client version is {0} but Server version is {1}", protocol_version, open_resp.ServerProtocolVersion);
                    Console.WriteLine(message);
                }
                if (open_resp.ServerProtocolVersion == 0){
                    throw new TException("Protocol not supported", null);
                }
                sessionId = open_resp.SessionId;
                var statement_task = client.requestStatementIdAsync(sessionId);
                statement_task.Wait();
                statementId = statement_task.Result;
            }
            catch(Exception e){
                transport.Close();
                Console.WriteLine("session closed because ", e);
            }
            if(zoneId != ""){
                set_time_zone(zoneId);
            }else{
                zoneId = get_time_zone();
            } 
            is_close = false;          

        }
        public bool is_open(){
            return !is_close;
        }
        public void close(){
            if(is_close){
                return;
            }
            var req = new TSCloseSessionReq(sessionId);
            try{
                var task = client.closeSessionAsync(req);
                task.Wait();
            }
            catch(TException e){
                var message = String.Format("Error occurs when closing session at server. Maybe server is down. Error message:{0}", e);
                Console.WriteLine(message);
            }
            finally{
                is_close = true;
                if (transport != null){
                    transport.Close();
                }
            }

        }
         
        public void set_time_zone(string zoneId){
            var req = new TSSetTimeZoneReq(sessionId, zoneId);
            try{
                var task = client.setTimeZoneAsync(req);
                task.Wait();
                var message = String.Format("setting time zone_id as {0}, message:{1}", zoneId, task.Result.Message);
                Console.WriteLine(message);
            }
            catch(TException e ){
                var message = String.Format("could not set time zone because {0}", e);
                Console.WriteLine(message);
                throw e; 
            }
            this.zoneId = zoneId;
        }
        public string get_time_zone(){
            TSGetTimeZoneResp resp;
            if(zoneId != ""){
                return zoneId;
            }
            try{
                var task = client.getTimeZoneAsync(sessionId);
                task.Wait();
                resp = task.Result;
            }
            catch(TException e){
                var message = String.Format("counld not get time zone beacuse {0}", e);
                Console.WriteLine(message);
                throw e; 
            }
            return resp.TimeZone;
        }
       
    }
}