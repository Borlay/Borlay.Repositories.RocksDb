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
    public class SecondaryRepositoryTests
    {
        [TestMethod]
        public void SaveAndEntityOrderDescGet()
        {
            var dbOptions = new RocksDbSharp.DbOptions();
            dbOptions.SetCreateIfMissing();
            var rocksDb = RocksDbSharp.RocksDb.Open(dbOptions, @"C:\rocks\secondaryrepositorytst\");
            var repository = new RocksSecondaryRepository(rocksDb, "ent");

            try
            {
                var parentId = ByteArray.New(32);

                var id1 = ByteArray.New(32);
                var value1 = ByteArray.New(256);
                var id2 = ByteArray.New(32);
                var value2 = ByteArray.New(256);
                var id3 = ByteArray.New(32);
                var value3 = ByteArray.New(256);

                using (var transaction = repository.CreateTransaction())
                {
                    var key1 = transaction.AppendValue(parentId, id1, value1.Bytes, value1.Length);
                    transaction.AppendScoreIndex(parentId, id1, key1, 12, IndexLevel.Entity, OrderType.Desc);

                    var key2 = transaction.AppendValue(parentId, id2, value2.Bytes, value2.Length);
                    transaction.AppendScoreIndex(parentId, id2, key2, 15, IndexLevel.Entity, OrderType.Desc);

                    var key3 = transaction.AppendValue(parentId, id3, value3.Bytes, value3.Length);
                    transaction.AppendScoreIndex(parentId, id3, key3, 14, IndexLevel.Entity, OrderType.Desc);

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
                Directory.Delete(@"C:\rocks\secondaryrepositorytst\", true);
            }
        }

        [TestMethod]
        public void SaveAndParentOrderDescGet()
        {
            var dbOptions = new RocksDbSharp.DbOptions();
            dbOptions.SetCreateIfMissing();
            var rocksDb = RocksDbSharp.RocksDb.Open(dbOptions, @"C:\rocks\secondaryrepositorytst\");
            var repository = new RocksSecondaryRepository(rocksDb, "ent");

            try
            {
                var parentId = ByteArray.New(32);

                var id1 = ByteArray.New(32);
                var value1 = ByteArray.New(256);
                var id2 = ByteArray.New(32);
                var value2 = ByteArray.New(256);
                var id3 = ByteArray.New(32);
                var value3 = ByteArray.New(256);

                using (var transaction = repository.CreateTransaction())
                {
                    var key1 = transaction.AppendValue(parentId, id1, value1.Bytes, value1.Length);
                    transaction.AppendScoreIndex(parentId, id1, key1, 14, IndexLevel.Parent, OrderType.Desc);

                    var key2 = transaction.AppendValue(parentId, id2, value2.Bytes, value2.Length);
                    transaction.AppendScoreIndex(parentId, id2, key2, 12, IndexLevel.Parent, OrderType.Desc);

                    var key3 = transaction.AppendValue(parentId, id3, value3.Bytes, value3.Length);
                    transaction.AppendScoreIndex(parentId, id3, key3, 15, IndexLevel.Parent, OrderType.Desc);

                    transaction.Commit();
                }

                var result = repository.GetValues(parentId, OrderType.Desc).ToArray();

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result.Length);

                Assert.IsTrue(result[0].Value.ContainsSequence32(value3.Bytes));
                Assert.IsTrue(result[1].Value.ContainsSequence32(value1.Bytes));
                Assert.IsTrue(result[2].Value.ContainsSequence32(value2.Bytes));

            }
            finally
            {
                rocksDb.Dispose();
                Directory.Delete(@"C:\rocks\secondaryrepositorytst\", true);
            }
        }


        [TestMethod]
        public void SaveAndEntityOrderAscGet()
        {
            var dbOptions = new RocksDbSharp.DbOptions();
            dbOptions.SetCreateIfMissing();
            var rocksDb = RocksDbSharp.RocksDb.Open(dbOptions, @"C:\rocks\secondaryrepositorytst\");
            var repository = new RocksSecondaryRepository(rocksDb, "ent");

            try
            {
                var parentId = ByteArray.New(32);

                var id1 = ByteArray.New(32);
                var value1 = ByteArray.New(256);
                var id2 = ByteArray.New(32);
                var value2 = ByteArray.New(256);
                var id3 = ByteArray.New(32);
                var value3 = ByteArray.New(256);

                using (var transaction = repository.CreateTransaction())
                {
                    var key1 = transaction.AppendValue(parentId, id1, value1.Bytes, value1.Length);
                    transaction.AppendScoreIndex(parentId, id1, key1, 12, IndexLevel.Entity, OrderType.Asc);

                    var key2 = transaction.AppendValue(parentId, id2, value2.Bytes, value2.Length);
                    transaction.AppendScoreIndex(parentId, id2, key2, 15, IndexLevel.Entity, OrderType.Asc);

                    var key3 = transaction.AppendValue(parentId, id3, value3.Bytes, value3.Length);
                    transaction.AppendScoreIndex(parentId, id3, key3, 14, IndexLevel.Entity, OrderType.Asc);

                    transaction.Commit();
                }

                var result = repository.GetValues(OrderType.Asc).ToArray();

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result.Length);

                Assert.IsTrue(result[0].Value.ContainsSequence32(value1.Bytes));
                Assert.IsTrue(result[1].Value.ContainsSequence32(value3.Bytes));
                Assert.IsTrue(result[2].Value.ContainsSequence32(value2.Bytes));
            }
            finally
            {
                rocksDb.Dispose();
                Directory.Delete(@"C:\rocks\secondaryrepositorytst\", true);
            }
        }

        [TestMethod]
        public void SaveAndParentOrderAscGet()
        {
            var dbOptions = new RocksDbSharp.DbOptions();
            dbOptions.SetCreateIfMissing();
            var rocksDb = RocksDbSharp.RocksDb.Open(dbOptions, @"C:\rocks\secondaryrepositorytst\");
            var repository = new RocksSecondaryRepository(rocksDb, "ent");

            try
            {
                var parentId = ByteArray.New(32);

                var id1 = ByteArray.New(32);
                var value1 = ByteArray.New(256);
                var id2 = ByteArray.New(32);
                var value2 = ByteArray.New(256);
                var id3 = ByteArray.New(32);
                var value3 = ByteArray.New(256);

                using (var transaction = repository.CreateTransaction())
                {
                    var key1 = transaction.AppendValue(parentId, id1, value1.Bytes, value1.Length);
                    transaction.AppendScoreIndex(parentId, id1, key1, 14, IndexLevel.Parent, OrderType.Asc);

                    var key2 = transaction.AppendValue(parentId, id2, value2.Bytes, value2.Length);
                    transaction.AppendScoreIndex(parentId, id2, key2, 12, IndexLevel.Parent, OrderType.Asc);

                    var key3 = transaction.AppendValue(parentId, id3, value3.Bytes, value3.Length);
                    transaction.AppendScoreIndex(parentId, id3, key3, 15, IndexLevel.Parent, OrderType.Asc);

                    transaction.Commit();
                }

                var result = repository.GetValues(parentId, OrderType.Asc, true).ToArray();
                var result2 = repository.GetValues(parentId, true).ToArray();

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result.Length);

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result2.Length);

                Assert.IsTrue(result[0].Value.ContainsSequence32(value2.Bytes));
                Assert.IsTrue(result[1].Value.ContainsSequence32(value1.Bytes));
                Assert.IsTrue(result[2].Value.ContainsSequence32(value3.Bytes));

            }
            finally
            {
                rocksDb.Dispose();
                Directory.Delete(@"C:\rocks\secondaryrepositorytst\", true);
            }
        }
    }
}
