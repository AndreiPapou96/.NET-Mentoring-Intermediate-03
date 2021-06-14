using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Messaging;
using System.IO;

namespace MessageServer
{
    class Program
    {
        const string MessageQueueName = @".\private$\MyTransactionalQueue";
        const string FolderPath = @"C:\Users\Andrei_Papou\result_folder";
        static MessageQueue queue;

        static void Main(string[] args)
        {
            Console.WriteLine("Server has started...");

            if (MessageQueue.Exists(MessageQueueName))
                queue = new MessageQueue(MessageQueueName);
            else
                queue = MessageQueue.Create(MessageQueueName, true);

            queue.Formatter = new XmlMessageFormatter(new Type[] { typeof(byte[]) });
            queue.MessageReadPropertyFilter = new MessagePropertyFilter
            {
                IsLastInTransaction = true,
                Label = true,
                Body = true,
            };

            Task.Run(ListenQueue);

            Console.ReadLine();

        }

        static void ListenQueue()
        {
            while (true)
            {
                using (var trans = new MessageQueueTransaction())
                {
                    try
                    {
                        var chunks = new List<byte>();
                        var fileName = string.Empty;
                        var index = 0;

                        trans.Begin();
                        Message message;

                        while ((message = queue.Receive(trans)) != null)
                        {
                            var chunk = (byte[])message.Body;
                            fileName = message.Label;
                            chunks.AddRange(chunk);

                            Console.WriteLine($"{fileName} - #{index}");
                            index++;

                            if (message.IsLastInTransaction)
                            {
                                break;
                            }
                        }

                        trans.Commit();
                        CombineChunksIntoSingleFile(chunks, FolderPath, fileName);
                    }

                    catch (MessageQueueException e)
                    {
                        if (e.MessageQueueErrorCode ==
                            MessageQueueErrorCode.TransactionUsage)
                        {
                            Console.WriteLine("Queue is not transactional.");
                        }

                        trans.Abort();
                    }
                }
            }
        }
        static void CombineChunksIntoSingleFile(List<byte> chunks, string folderPath, string fileName)
        {
            using (var input = new FileStream($"{folderPath}\\{fileName}", FileMode.Create, FileAccess.Write))
            {
                foreach (byte chunk in chunks)
                {
                    input.WriteByte(chunk);
                }
            }
        }
    }
}
