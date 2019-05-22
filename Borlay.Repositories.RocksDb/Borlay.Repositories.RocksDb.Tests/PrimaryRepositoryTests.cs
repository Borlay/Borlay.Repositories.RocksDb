using Borlay.Arrays;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using System.Linq;

namespace Borlay.Repositories.RocksDb.Tests
{
    [TestClass]
    public class PrimaryRepositoryTests
    {
        [TestMethod]
        public void SaveAndEntityOrderDescGet()
        {
            var dbOptions = new RocksDbSharp.DbOptions();
            dbOptions.SetCreateIfMissing();
            var rocksDb = RocksDbSharp.RocksDb.Open(dbOptions, @"C:\rocks\primaryrepositorytst\");
            var repository = new RocksPrimaryRepository(rocksDb, "ent");

            try
            {
                var id1 = ByteArray.New(32);
                var value1 = ByteArray.New(256);
                var id2 = ByteArray.New(32);
                var value2 = ByteArray.New(256);
                var id3 = ByteArray.New(32);
                var value3 = ByteArray.New(256);

                using (var transaction = repository.CreateTransaction())
                {
                    var key1 = transaction.AppendValue(id1, value1.Bytes, value1.Length);
                    transaction.AppendScoreIndex(id1, key1, 12, IndexLevel.Entity, OrderType.Desc);

                    var key2 = transaction.AppendValue(id2, value2.Bytes, value2.Length);
                    transaction.AppendScoreIndex(id2, key2, 15, IndexLevel.Entity, OrderType.Desc);

                    var key3 = transaction.AppendValue(id3, value3.Bytes, value3.Length);
                    transaction.AppendScoreIndex(id3, key3, 14, IndexLevel.Entity, OrderType.Desc);

                    transaction.Commit();
                }

                var result = repository.GetValues(OrderType.Desc).ToArray();

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result.Length);

                Assert.IsTrue(result[0].Value.ContainsSequence32(value2.Bytes));
                Assert.IsTrue(result[1].Value.ContainsSequence32(value3.Bytes));
                Assert.IsTrue(result[2].Value.ContainsSequence32(value1.Bytes));

            }
            finally
            {
                rocksDb.Dispose();
                Directory.Delete(@"C:\rocks\primaryrepositorytst\", true);
            }
        }


        [TestMethod]
        public void SaveAndEntityOrderAscGet()
        {
            var dbOptions = new RocksDbSharp.DbOptions();
            dbOptions.SetCreateIfMissing();
            var rocksDb = RocksDbSharp.RocksDb.Open(dbOptions, @"C:\rocks\primaryrepositorytst\");
            var repository = new RocksPrimaryRepository(rocksDb, "ent");

            try
            {

                var id1 = ByteArray.New(32);
                var value1 = ByteArray.New(256);
                var id2 = ByteArray.New(32);
                var value2 = ByteArray.New(256);
                var id3 = ByteArray.New(32);
                var value3 = ByteArray.New(256);

                using (var transaction = repository.CreateTransaction())
                {
                    var key1 = transaction.AppendValue(id1, value1.Bytes, value1.Length);
                    transaction.AppendScoreIndex(id1, key1, 12, IndexLevel.Entity, OrderType.Asc);

                    var key2 = transaction.AppendValue(id2, value2.Bytes, value2.Length);
                    transaction.AppendScoreIndex(id2, key2, 15, IndexLevel.Entity, OrderType.Asc);

                    var key3 = transaction.AppendValue(id3, value3.Bytes, value3.Length);
                    transaction.AppendScoreIndex(id3, key3, 14, IndexLevel.Entity, OrderType.Asc);

                    transaction.Commit();
                }

                var result = repository.GetValues(OrderType.Asc, false).ToArray();

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result.Length);

                Assert.IsTrue(result[0].Value.ContainsSequence32(value1.Bytes));
                Assert.IsTrue(result[1].Value.ContainsSequence32(value3.Bytes));
                Assert.IsTrue(result[2].Value.ContainsSequence32(value2.Bytes));

            }
            finally
            {
                rocksDb.Dispose();
                Directory.Delete(@"C:\rocks\primaryrepositorytst\", true);
            }
        }
    }
}
