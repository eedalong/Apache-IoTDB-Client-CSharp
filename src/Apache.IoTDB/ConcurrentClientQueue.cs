using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Apache.IoTDB
{
    public class ConcurrentClientQueue
    {
        public ConcurrentQueue<Client> ClientQueue { get; }

        public ConcurrentClientQueue(List<Client> clients)
        {
            ClientQueue = new ConcurrentQueue<Client>(clients);
        }
        public ConcurrentClientQueue()
        {
            ClientQueue = new ConcurrentQueue<Client>();
        }
        public void Add(Client client) => Return(client);

        public void Return(Client client)
        {
            Monitor.Enter(ClientQueue);
            ClientQueue.Enqueue(client);
            Monitor.PulseAll(ClientQueue); // wake up all threads waiting on the queue, refresh the waiting time
            Monitor.Exit(ClientQueue);
            Thread.Sleep(0);
        }
        int _ref = 0;
        public void AddRef()
        {
            lock (this)
            {
                _ref++;
            }
        }
        public int GetRef()
        {
            return _ref;
        }
        public void RemoveRef()
        {
            lock (this)
            {
                _ref--;
            }
        }
        public int Timeout { get; set; } = 10;
        public Client Take()
        {
            Client client = null;
            Monitor.Enter(ClientQueue);
            while(true){
                bool timeout = false;
                if (ClientQueue.IsEmpty)
                {
                    timeout = !Monitor.Wait(ClientQueue, TimeSpan.FromSeconds(Timeout));
                }
                ClientQueue.TryDequeue(out client);

                if(client != null || timeout){
                    break;
                }
            }
            Monitor.Exit(ClientQueue);
            if (client == null)
            {
                throw new TimeoutException($"Connection pool is empty and wait time out({Timeout}s)!");
            }
            return client;
        }
    }
}