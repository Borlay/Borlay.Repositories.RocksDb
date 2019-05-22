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

        public virtual byte[] GetValue(ByteArray entityId)
        {
            var key = IndexGenerator.GetEntityKey(entityId);
            var value = Database.Get(key);
            return value;
        }

        public virtual KeyValuePair<byte[], byte[]>[] GetValues(ByteArray[] entityIds)
        {
            var keys = entityIds.Select(id => IndexGenerator.GetEntityKey(id)).ToArray();
            var values = Database.MultiGet(keys);
            return values;

            //var result = new KeyValuePair<byte[], byte[]>[values.Length];

            //for (int i = 0; i < result.Length; i++)
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

        //public virtual KeyValuePair<ByteArray, byte[]>[] Get(ByteArray[] entityIds)
        //{
        //    var keys = entityIds.Select(id => IndexGenerator.GetEntityKey(id)).ToArray();
        //    var values = Database.MultiGet(keys);

        //    var result = new KeyValuePair<ByteArray, byte[]>[values.Length];

        //    for (int i = 0; i < result.Length; i++)
        //    {
        //        ByteArray id = null;
        //        if (entityIds[i].Bytes.ContainsSequence32(values[i].Key))
        //            id = entityIds[i];
        //        else
        //            id = new ByteArray(values[i].Key);

        //        result[i] = new KeyValuePair<ByteArray, byte[]>(id, values[i].Value);
        //    }

        //    return result;
        //}

        public virtual IEnumerable<KeyValuePair<byte[], byte[]>> GetValues(OrderType orderType, bool distinct = false)
        {
            var skey = IndexGenerator.GetEntityOrderKey(orderType);
            return Database.Search(skey, distinct, true);
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> GetValues(bool distinct = false)
        {
            var skey = IndexGenerator.GetEntityName();
            return Database.Search(skey, distinct, false);
        }

        public bool Contains(ByteArray entityId)
        {
            var key = IndexGenerator.GetEntityKey(entityId);
            var value = Database.Get(key);
            return value != null;
        }

        public IPrimaryTransaction CreateTransaction()
        {
            return new RocksPrimaryTransaction(this);
        }
    }
}
