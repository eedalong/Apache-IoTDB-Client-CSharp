using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Apache.IoTDB
{
    public class ConcurrentClientQueue
    {
        public ConcurrentQueue<Client> client_queue;

        public ConcurrentClientQueue(List<Client> clients)
        {
            client_queue = new ConcurrentQueue<Client>(clients);
        }

        public ConcurrentClientQueue()
        {
            client_queue = new ConcurrentQueue<Client>();
        }

        public void Add(Client client)
        {
            Monitor.Enter(client_queue);
            client_queue.Enqueue(client);
            Monitor.Pulse(client_queue);
            Monitor.Exit(client_queue);
        }

        public Client Take()
        {
            Client client;
            Monitor.Enter(client_queue);
            if (client_queue.IsEmpty)
            {
                Monitor.Wait(client_queue);
            }

            client_queue.TryDequeue(out client);
            Monitor.Exit(client_queue);
            return client;
        }
    }
}