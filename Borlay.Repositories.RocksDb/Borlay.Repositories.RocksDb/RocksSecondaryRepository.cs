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

        public virtual byte[] Get(ByteArray parentId, ByteArray entityId)
        {
            var key = IndexGenerator.GetParentEntityKey(parentId, entityId);
            var value = Database.Get(key);
            return value;
        }

        public virtual KeyValuePair<ByteArray, byte[]>[] Get(ByteArray parentId, ByteArray[] entityIds)
        {
            var keys = entityIds.Select(id => IndexGenerator.GetParentEntityKey(parentId, id)).ToArray();
            var values = Database.MultiGet(keys);

            var result = new KeyValuePair<ByteArray, byte[]>[values.Length];

            for(int i = 0; i < result.Length; i++)
            {
                ByteArray id = null;
                if (entityIds[i].Bytes.ContainsSequence32(values[i].Key))
                    id = entityIds[i];
                else
                    id = new ByteArray(values[i].Key);

                result[i] = new KeyValuePair<ByteArray, byte[]>(id, values[i].Value);
            }

            return result;
        }

        public virtual IEnumerable<byte[]> Get(ByteArray parentId, OrderType orderType)
        {
            var skey = IndexGenerator.GetParentOrderKey(parentId, orderType);
            return Database.Search(skey);
        }

        public virtual IEnumerable<byte[]> Get(OrderType orderType)
        {
            var skey = IndexGenerator.GetEntityOrderKey(orderType);
            return Database.Search(skey);
        }


        public ISecondaryTransaction CreateTransaction()
        {
            return new RocksSecondaryTransaction(this);
        }

        public IEnumerable<byte[]> Get(ByteArray parentId)
        {
            var key = IndexGenerator.GetParentEntityName(parentId);
            return Database.Search(key);
        }

        public bool Contains(ByteArray parentId, ByteArray entityId)
        {
            var key = IndexGenerator.GetParentEntityKey(parentId, entityId);
            var value = Database.Get(key);
            return value != null;
        }
    }
}
