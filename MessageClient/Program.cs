using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Messaging;
using System.IO;

namespace MessageClient
{
    class Program
    {
        const string MessageQueueName = @".\private$\MyTransactionalQueue";
        const string FolderPath = @"C:\Users\Andrei_Papou\Listen_Folder";
        const int BUFFER_SIZE = 3;
        static MessageQueue queue;

        static void Main(string[] args)
        {
            Console.WriteLine("Client has started...");

            if (MessageQueue.Exists(MessageQueueName))
                queue = new MessageQueue(MessageQueueName);
            else
                queue = MessageQueue.Create(MessageQueueName);

            queue.Formatter = new XmlMessageFormatter(new Type[] { typeof(byte[]) });

            FileSystemWatcher watcher = new FileSystemWatcher(FolderPath);

            watcher.EnableRaisingEvents = true;
            watcher.Filter = "*.zip";
            watcher.Created += SendFileToQueue;

            Console.ReadLine();
        }

        static void SendFileToQueue(object sender, FileSystemEventArgs e)
        {
            if (queue.Transactional == true)
            {
                // Create a transaction.
                MessageQueueTransaction myTransaction = new
                    MessageQueueTransaction();

                // Begin the transaction.
                myTransaction.Begin();

                byte[] buffer = new byte[BUFFER_SIZE];

                using (var input = new FileStream(e.FullPath, FileMode.Open, FileAccess.Read))
                {
                    int index = 0;

                    while (input.Position < input.Length)
                    {
                        int remaining = 3, bytesRead;
                        while (remaining > 0 && (bytesRead = input.Read(buffer, 0,
                                Math.Min(remaining, BUFFER_SIZE))) > 0)
                        {
                            queue.Send(buffer, e.Name, myTransaction);
                            remaining -= bytesRead;
                        }

                        index++;
                    }
                }

                // Commit the transaction.
                myTransaction.Commit();
            }
            else
            {
                queue.Send("My Message Data.");
            }
        }
    }
}
