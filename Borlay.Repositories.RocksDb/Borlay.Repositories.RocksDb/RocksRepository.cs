using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public class RocksRepository
    {
        public RocksDbSharp.RocksDb db { get; }
        public WriteOptions writeOptions { get; }
        public OrderType[] indexOrders { get; } = new OrderType[256];

        public IndexGenerator indexGenerator { get; }

        public RocksRepository(RocksDbSharp.RocksDb db, IndexGenerator indexGenerator)
        {
            this.db = db;

            writeOptions = new WriteOptions();
            //writeOptions.SetSync(true);

            this.indexGenerator = indexGenerator;
        }

        public void SetOrder(OrderIndexType indexType, OrderType orderType)
        {
            indexOrders[(byte)indexType] = orderType;
        }

        public OrderType GetOrder(OrderIndexType indexType)
        {
            return indexOrders[(byte)indexType];
        }

        public virtual async Task<byte[]> Get(ByteArray userId, ByteArray entityId)
        {
            var key = indexGenerator.GetUserEntityKey(userId, entityId, DataType.Entity);
            var value = db.Get(key);
            return value;
        }

        public virtual async Task<KeyValuePair<ByteArray, byte[]>[]> Get(ByteArray userId, ByteArray[] entityIds)
        {
            var keys = entityIds.Select(id => indexGenerator.GetUserEntityKey(userId, id, DataType.Entity)).ToArray();
            var values = db.MultiGet(keys);

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

        public virtual Task<T[]> Get(ByteArray userId, int skip, int take)
        {
            var skey = indexGenerator.GetOrderKey(userId, DataType.Index);
            return Get((byte[])skey, (byte[] entityIdBytes) => base.GetUserEntityKey(userId, entityIdBytes, 0), skip, take);
        }

        //public virtual Task<T[]> Get(int skip, int take)
        //{
        //    var skey = GetKey(userId, 1);
        //    return Get(skey, entityIdBytes => GetKey(userId, entityIdBytes, 0), skip, take);
        //}

        protected virtual async Task<byte[][]> Get(byte[] skey, Func<byte[], byte[]> resolveKey, int skip, int take)
        {
            //var skey = GetKey(userId, 1);
            using (var iterator = db.NewIterator())
            {
                List<byte[]> list = new List<byte[]>();

                var it = iterator.Seek(skey);
                while (it.Valid() && take != 0)
                {
                    if (!it.Key().ContainsSequence32(skey)) break;

                    if (skip <= 0)
                    {
                        var entityIdBytes = it.Value();

                        var key = resolveKey(entityIdBytes);
                        var value = db.Get(key);

                        //var index = 0;
                        //var entity = serializer.GetObject(value, ref index);
                        list.Add(value);
                        take--;

                        if (take == 0) break;
                    }
                    else
                        skip--;

                    it = it.Next();
                }

                return list.ToArray();
            }
        }


        public IRepositoryTransaction CreateTransaction()
        {
            return new RocksRepositoryTransaction(this);
        }
    }

    public interface IOrderIndex
    {
        OrderIndexType Type { get; }
    }

    public class OrderIndex : IOrderIndex
    {
        public OrderIndexType Type { get; set; }
    }

    public interface IScoreIndex : IOrderIndex
    {
        long Score { get; }
    }

    public interface IDateIndex : IOrderIndex
    {
        DateTime Date { get; }
    }

    public class ScoreOrderIndex : OrderIndex, IScoreIndex
    {
        public long Score { get; set; }
    }

    public class DateOrderIndex : OrderIndex, IDateIndex
    {
        public DateTime Date { get; set; }
    }
}
