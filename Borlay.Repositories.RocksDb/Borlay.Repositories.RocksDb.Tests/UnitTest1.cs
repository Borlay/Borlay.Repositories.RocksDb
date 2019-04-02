using Borlay.Arrays;
using Borlay.Serialization.Notations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb.Tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public async Task SaveAndGet()
        {
            var serializer = new Serialization.Converters.Serializer();
            serializer.LoadFromReference<UnitTest1>();

            var dbOptions = new RocksDbSharp.DbOptions();
            dbOptions.SetCreateIfMissing();
            var rocksDb = RocksDbSharp.RocksDb.Open(dbOptions, @"C:\rocks\secondaryrepositoriestst\");

            var repository = new SortedSecondaryRepository<TestEntity>(rocksDb, serializer);

            try
            {

                var e1 = Create("e1", 110);
                var e2 = Create("e2", 120);
                var e3 = Create("e3", 130);
                var e4 = Create("e4", 140);

                var e5 = Create("e5", 150);
                var e6 = Create("e6", 160);

                var user1 = ByteArray.New(32);
                var user2 = ByteArray.New(32);

                await repository.Save(user1, e1);
                await repository.Save(user1, e2);
                await repository.Save(user1, e3);
                await repository.Save(user1, e4);
                await repository.Save(user2, e5);
                await repository.Save(user2, e6);

                var ge1 = await repository.Get(user1, e1.Id);

                var u1ge = await repository.Get(user1, 0, 10);
                var u2ge = await repository.Get(user2, 0, 10);

                Assert.AreEqual(4, u1ge.Length);
                Assert.AreEqual(2, u2ge.Length);

                Assert.AreEqual("e4", u1ge[0].Name);
                Assert.AreEqual("e6", u2ge[0].Name);

            }
            finally
            {
                rocksDb.Dispose();
                Directory.Delete(@"C:\rocks\secondaryrepositoriestst\", true);
            }
        }

        [TestMethod]
        public async Task SaveMany()
        {
            var serializer = new Serialization.Converters.Serializer();
            serializer.LoadFromReference<UnitTest1>();

            var dbOptions = new RocksDbSharp.DbOptions();
            dbOptions.SetCreateIfMissing();
            var rocksDb = RocksDbSharp.RocksDb.Open(dbOptions, @"C:\rocks\secondaryrepositoriestst\");

            var repository = new SortedSecondaryRepository<TestEntity>(rocksDb, serializer);

            repository.AllowOrderDublicates = true;

            try
            {

                var e1 = Create("e1", 110);
                var user1 = ByteArray.New(32);

                var watch = Stopwatch.StartNew();

                for (int i = 0; i < 10000; i++)
                {
                    e1.Score = i;
                    await repository.Save(user1, e1);

                    var ge1 = await repository.Get(user1, e1.Id);

                    var u1ge = await repository.Get(user1, 0, 10);
                }

                watch.Stop();

                // not allow
                // 10k 0.85s + del - 2.s

                // allow
                // 10k 0.85s
            }
            finally
            {
                rocksDb.Dispose();
                Directory.Delete(@"C:\rocks\secondaryrepositoriestst\", true);
            }
        }

        public TestEntity Create(string name, long score)
        {
            return new TestEntity()
            {
                Id = ByteArray.New(32),
                Name = name,
                Score = score
            };
        }
    }

    [Data(2000)]
    public class TestEntity : IScoreEntity
    {
        [Include(0, true)]
        public ByteArray Id { get; set; }

        [Include(1, true)]
        public string Name { get; set; }

        public long Score { get; set; }
    }
}
