using System;
using System.Linq;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace Akkanet.Streams.Template
{
    class Program
    {
        static void Main(string[] args)
        {
            var result = Stream();
            result.Wait();
            Console.WriteLine(result.Result);
        }

        static Task<int> Stream()
        {
            // 1秒ごとに下流に各要素を放出する Source
            Source<int, NotUsed> source = Source.From(Enumerable.Range(1, 100));

            // 上流から流れてきた要素を足し合わせる Sink
            Sink<int, Task<int>> sink = Sink.Aggregate<int, int>(0, (l, r) => l + r);

            // Stream を正常（もしくはキャンセル扱いで）に停止させるための KillSwitch
            Flow<int, int, UniqueKillSwitch> killSwitch = Flow.Create<int>().ViaMaterialized(KillSwitches.Single<int>(), Keep.Right);

            // Stream（の特定の部分）を通過する要素の流量を制御するための Throttle
            Flow<int, int, NotUsed> throttle = Flow.Create<int>().Throttle(1, TimeSpan.FromSeconds(1), 1, ThrottleMode.Shaping);

            // Stream を動作させる Actor をホストする ActorSystem
            ActorSystem system = ActorSystem.Create("akkanet");

            // ActorSystem を使用して Stream をマテリアル化するマテリアライザ
            ActorMaterializer materializer = ActorMaterializer.Create(system);

            // Source、Sink、Throttle、KillSwitch を使用して RunnableGraph（実行可能なグラフ）を組み立てる
            IRunnableGraph<Tuple<UniqueKillSwitch, Task<int>>> runnable =
                source
                    .Via(throttle)
                    .ViaMaterialized(killSwitch, Keep.Right)
                    .ToMaterialized(sink, Keep.Both);

            // RunnableGraph をマテリアライズして Stream を作動させる
            var (ks, mat) = runnable.Run(materializer);

            // 10秒後に KillSwitch を使用して Stream を途中で停止させる（完了扱い）
            ICancelable canceller = materializer.ScheduleOnce(TimeSpan.FromSeconds(10), () =>
            {
                Console.WriteLine("Stream is cancelled");
                ks.Shutdown();
            });

            // Stream 完了後に ActorSystem と ActorMaterializer を破棄する
            return mat.ContinueWith(prev =>
            {
                canceller.Cancel();
                materializer.Dispose();
                system.Dispose();

                return prev.Result;
            });
        }
    }
}
