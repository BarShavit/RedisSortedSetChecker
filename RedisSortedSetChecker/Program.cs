using System;
using System.Collections.Generic;
using System.Threading;
using StackExchange.Redis;

namespace ConsoleApp6

{
    public class Program
    {
        public static void Main(string[] args)
        {
            var received = new List<int>[10];

            for (var i = 0; i < received.Length; i++)
            {
                received[i] = new List<int>();
            }

            var conf = new ConfigurationOptions
            {
                AbortOnConnectFail = false
            };
            conf.EndPoints.Add("localhost:6379");

            var interval = 50;
            var cts = new CancellationTokenSource();

            var key = "test";

            for (var i = 0; i < received.Length; i++)
            {
                var i1 = i;

                var t = new Thread(() =>

                {
                    var redis = ConnectionMultiplexer.Connect(conf);

                    var db = redis.GetDatabase(0);

                    while (!cts.IsCancellationRequested)

                    {
                        var r = db.SortedSetPop(key);

                        if (r == null)
                        {
                            Thread.Sleep(interval);

                            continue;

                        }

                        var value = int.Parse(r.Value.Element);

                        received[i1].Add(value);

                        Console.WriteLine(value);
                    }
                })
                {
                    IsBackground = true
                };

                t.Start();

            }

            var sendThread = new Thread(() =>

            {
                var redis = ConnectionMultiplexer.Connect(conf);
                var db = redis.GetDatabase(0);
                var counter = 0;

                while (!cts.IsCancellationRequested)
                {
                    db.SortedSetAdd(key, counter.ToString(), counter);

                    Thread.Sleep(20);

                    counter++;

                }
            })
            {
                IsBackground = true
            };

            sendThread.Start();

            Thread.Sleep(900000);

            cts.Cancel();

            var hashSet = new HashSet<int>();

            foreach (var t in received)
            {
                foreach (var num in t)

                {
                    var result = hashSet.Add(num);

                    if (!result)
                        Console.WriteLine($"FOUNDDDDDDD {num}");

                }

            }

            Console.WriteLine("Finished");

            Console.Read();
        }
    }
}