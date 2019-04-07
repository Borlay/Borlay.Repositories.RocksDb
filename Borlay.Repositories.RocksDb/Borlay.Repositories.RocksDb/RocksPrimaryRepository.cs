using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Borlay.Repositories.RocksDb
{
    public class RocksPrimaryRepository : IPrimaryRepository
    {
        public RocksDbSharp.RocksDb Database { get; }
        public WriteOptions WriteOptions { get; }
        public PrimaryIndexGenerator IndexGenerator { get; }
        public IIndexOrderProvider IndexOrders { get; set; }

        public RocksPrimaryRepository(RocksDbSharp.RocksDb db, string entityName)
            : this(db, new PrimaryIndexGenerator(entityName), new IndexOrderProvider())
        {
        }

        public RocksPrimaryRepository(RocksDbSharp.RocksDb db, PrimaryIndexGenerator indexGenerator)
            : this(db, indexGenerator, new IndexOrderProvider())
        {
        }

        public RocksPrimaryRepository(RocksDbSharp.RocksDb db, PrimaryIndexGenerator indexGenerator, IIndexOrderProvider indexOrders)
        {
            this.Database = db;

            WriteOptions = new WriteOptions();
            this.IndexGenerator = indexGenerator;
            this.IndexOrders = indexOrders;
        }

        public virtual byte[] Get(ByteArray entityId)
        {
            var key = IndexGenerator.GetEntityKey(entityId);
            var value = Database.Get(key);
            return value;
        }

        public virtual KeyValuePair<ByteArray, byte[]>[] Get(ByteArray[] entityIds)
        {
            var keys = entityIds.Select(id => IndexGenerator.GetEntityKey(id)).ToArray();
            var values = Database.MultiGet(keys);

            var result = new KeyValuePair<ByteArray, byte[]>[values.Length];

            for (int i = 0; i < result.Length; i++)
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

        public virtual IEnumerable<byte[]> Get(OrderType orderType)
        {
            var skey = IndexGenerator.GetEntityOrderKey(orderType);
            return Database.Search(skey);
        }

        public IPrimaryTransaction CreateTransaction()
        {
            return new RocksPrimaryTransaction(this);
        }
    }
}
