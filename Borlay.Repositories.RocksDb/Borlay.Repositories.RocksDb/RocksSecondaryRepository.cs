using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public class RocksSecondaryRepository : ISecondaryRepository
    {
        public RocksDbSharp.RocksDb Database { get; }
        public WriteOptions WriteOptions { get; }
        public SecondaryIndexGenerator IndexGenerator { get; }
        public IIndexOrderProvider IndexOrders { get; set; }

        public RocksSecondaryRepository(RocksDbSharp.RocksDb db, string entityName)
            : this(db, new SecondaryIndexGenerator(entityName), new IndexOrderProvider())
        {
        }

        public RocksSecondaryRepository(RocksDbSharp.RocksDb db, SecondaryIndexGenerator indexGenerator)
            : this(db, indexGenerator, new IndexOrderProvider())
        {
        }

        public RocksSecondaryRepository(RocksDbSharp.RocksDb db, SecondaryIndexGenerator indexGenerator, IIndexOrderProvider indexOrders)
        {
            this.Database = db;

            WriteOptions = new WriteOptions();
            this.IndexGenerator = indexGenerator;
            this.IndexOrders = indexOrders;
        }

        public virtual byte[] GetValue(ByteArray parentId, ByteArray entityId)
        {
            var key = IndexGenerator.GetParentEntityKey(parentId, entityId);
            var value = Database.Get(key);
            return value;
        }

        public virtual KeyValuePair<byte[], byte[]>[] GetValues(ByteArray parentId, ByteArray[] entityIds)
        {
            var keys = entityIds.Select(id => IndexGenerator.GetParentEntityKey(parentId, id)).ToArray();
            var values = Database.MultiGet(keys);
            return values;

            //var result = new KeyValuePair<ByteArray, byte[]>[values.Length];

            //for(int i = 0; i < result.Length; i++)
            //{
            //    ByteArray id = null;
            //    if (entityIds[i].Bytes.ContainsSequence32(values[i].Key))
            //        id = entityIds[i];
            //    else
            //        id = new ByteArray(values[i].Key);

            //    result[i] = new KeyValuePair<ByteArray, byte[]>(id, values[i].Value);
            //}

            //return result;
        }

        public virtual IEnumerable<KeyValuePair<byte[], byte[]>> GetValues(ByteArray parentId, OrderType orderType, bool distinct = false)
        {
            var skey = IndexGenerator.GetParentOrderKey(parentId, orderType);
            return Database.Search(skey, distinct, true);
        }

        public virtual IEnumerable<KeyValuePair<byte[], byte[]>> GetValues(OrderType orderType, bool distinct = false)
        {
            var skey = IndexGenerator.GetEntityOrderKey(orderType);
            return Database.Search(skey, distinct, true);
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> GetValues(ByteArray parentId, bool distinct = false)
        {
            var key = IndexGenerator.GetParentEntityName(parentId);
            return Database.Search(key, distinct, false);
        }

        public bool Contains(ByteArray parentId, ByteArray entityId)
        {
            var key = IndexGenerator.GetParentEntityKey(parentId, entityId);
            var value = Database.Get(key);
            return value != null;
        }

        public ISecondaryTransaction CreateTransaction()
        {
            return new RocksSecondaryTransaction(this);
        }
    }
}
