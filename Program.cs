using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Startup
{
    class Program
    {
        static void Main(string[] args)
        {
            var queue = new BlockingCollection<long>();
            var dict = new ConcurrentDictionary<long, ManualResetEvent>();
            var producerCount = 2;
            var consumerCount = 1;
            var messageCountOfSingleProducer = 1000000;
            var printBatchSize = 100000;
            var producerActions = new List<Action>();
            var consumerActions = new List<Action>();
            var baseKey = 0L;

            var producerAction = new Action(() =>
            {
                var watch = Stopwatch.StartNew();
                var threadId = Thread.CurrentThread.ManagedThreadId;
                Console.WriteLine("T:{0}, send message start...", threadId);
                for (var i = 1; i <= messageCountOfSingleProducer; i++)
                {
                    var waitHandle = new ManualResetEvent(false);
                    var nextKey = Interlocked.Increment(ref baseKey);
                    dict.TryAdd(nextKey, waitHandle);
                    queue.Add(nextKey);
                    waitHandle.WaitOne();
                    if (i % printBatchSize == 0)
                    {
                        Console.WriteLine("T:{0}, send {1} messages, time spent: {2}ms", threadId, i, watch.ElapsedMilliseconds);
                    }
                }
                Console.WriteLine("T:{0}, send message end..., time spent: {1}ms", threadId, watch.ElapsedMilliseconds);
            });
            var consumerAction = new Action(() =>
            {
                while (true)
                {
                    ManualResetEvent waitHandle;
                    dict.TryGetValue(queue.Take(), out waitHandle);
                    waitHandle.Set();
                }
            });

            for (var i = 0; i < producerCount; i++)
            {
                producerActions.Add(producerAction);
            }
            for (var i = 0; i < consumerCount; i++)
            {
                consumerActions.Add(consumerAction);
            }

            Task.Factory.StartNew(() => Parallel.Invoke(consumerActions.ToArray()));
            Task.Factory.StartNew(() => Parallel.Invoke(producerActions.ToArray()));

            Console.ReadLine();
        }
    }
}
