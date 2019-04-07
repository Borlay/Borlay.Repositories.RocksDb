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
                    transaction.AppendOrderIndex(parentId, id1, key1, IndexLevel.Entity, 12, OrderType.Desc);

                    var key2 = transaction.AppendValue(parentId, id2, value2.Bytes, value2.Length);
                    transaction.AppendOrderIndex(parentId, id2, key2, IndexLevel.Entity, 15, OrderType.Desc);

                    var key3 = transaction.AppendValue(parentId, id3, value3.Bytes, value3.Length);
                    transaction.AppendOrderIndex(parentId, id3, key3, IndexLevel.Entity, 14, OrderType.Desc);

                    transaction.Commit();
                }

                var result = repository.Get(OrderType.Desc).ToArray();

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result.Length);

                Assert.IsTrue(result[0].ContainsSequence32(value2.Bytes));
                Assert.IsTrue(result[1].ContainsSequence32(value3.Bytes));
                Assert.IsTrue(result[2].ContainsSequence32(value1.Bytes));
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
                    transaction.AppendOrderIndex(parentId, id1, key1, IndexLevel.Parent, 14, OrderType.Desc);

                    var key2 = transaction.AppendValue(parentId, id2, value2.Bytes, value2.Length);
                    transaction.AppendOrderIndex(parentId, id2, key2, IndexLevel.Parent, 12, OrderType.Desc);

                    var key3 = transaction.AppendValue(parentId, id3, value3.Bytes, value3.Length);
                    transaction.AppendOrderIndex(parentId, id3, key3, IndexLevel.Parent, 15, OrderType.Desc);

                    transaction.Commit();
                }

                var result = repository.Get(parentId, OrderType.Desc).ToArray();

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result.Length);

                Assert.IsTrue(result[0].ContainsSequence32(value3.Bytes));
                Assert.IsTrue(result[1].ContainsSequence32(value1.Bytes));
                Assert.IsTrue(result[2].ContainsSequence32(value2.Bytes));

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
                    transaction.AppendOrderIndex(parentId, id1, key1, IndexLevel.Entity, 12, OrderType.Asc);

                    var key2 = transaction.AppendValue(parentId, id2, value2.Bytes, value2.Length);
                    transaction.AppendOrderIndex(parentId, id2, key2, IndexLevel.Entity, 15, OrderType.Asc);

                    var key3 = transaction.AppendValue(parentId, id3, value3.Bytes, value3.Length);
                    transaction.AppendOrderIndex(parentId, id3, key3, IndexLevel.Entity, 14, OrderType.Asc);

                    transaction.Commit();
                }

                var result = repository.Get(OrderType.Asc).ToArray();

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result.Length);

                Assert.IsTrue(result[0].ContainsSequence32(value1.Bytes));
                Assert.IsTrue(result[1].ContainsSequence32(value3.Bytes));
                Assert.IsTrue(result[2].ContainsSequence32(value2.Bytes));
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
                    transaction.AppendOrderIndex(parentId, id1, key1, IndexLevel.Parent, 14, OrderType.Asc);

                    var key2 = transaction.AppendValue(parentId, id2, value2.Bytes, value2.Length);
                    transaction.AppendOrderIndex(parentId, id2, key2, IndexLevel.Parent, 12, OrderType.Asc);

                    var key3 = transaction.AppendValue(parentId, id3, value3.Bytes, value3.Length);
                    transaction.AppendOrderIndex(parentId, id3, key3, IndexLevel.Parent, 15, OrderType.Asc);

                    transaction.Commit();
                }

                var result = repository.Get(parentId, OrderType.Asc).ToArray();

                Assert.IsNotNull(result);
                Assert.AreEqual(3, result.Length);

                Assert.IsTrue(result[0].ContainsSequence32(value2.Bytes));
                Assert.IsTrue(result[1].ContainsSequence32(value1.Bytes));
                Assert.IsTrue(result[2].ContainsSequence32(value3.Bytes));

            }
            finally
            {
                rocksDb.Dispose();
                Directory.Delete(@"C:\rocks\secondaryrepositorytst\", true);
            }
        }
    }
}
