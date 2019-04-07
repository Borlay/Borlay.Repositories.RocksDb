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
        public RocksDbSharp.RocksDb Database { get; }
        public WriteOptions WriteOptions { get; }
        public IndexGenerator IndexGenerator { get; }
        public IIndexOrderProvider IndexOrders { get; set; }


        public RocksRepository(RocksDbSharp.RocksDb db, IndexGenerator indexGenerator)
            : this(db, indexGenerator, new IndexOrderProvider())
        {
        }

        public RocksRepository(RocksDbSharp.RocksDb db, IndexGenerator indexGenerator, IIndexOrderProvider indexOrders)
        {
            this.Database = db;

            WriteOptions = new WriteOptions();
            //writeOptions.SetSync(true);

            this.IndexGenerator = indexGenerator;
            this.IndexOrders = indexOrders;
        }

        public virtual async Task<byte[]> Get(ByteArray userId, ByteArray entityId)
        {
            var key = IndexGenerator.GetParentEntityKey(userId, entityId);
            var value = Database.Get(key);
            return value;
        }

        public virtual async Task<KeyValuePair<ByteArray, byte[]>[]> Get(ByteArray userId, ByteArray[] entityIds)
        {
            var keys = entityIds.Select(id => IndexGenerator.GetParentEntityKey(userId, id)).ToArray();
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
            return Get(skey);
        }

        public virtual IEnumerable<byte[]> Get(OrderType orderType)
        {
            var skey = IndexGenerator.GetEntityOrderKey(orderType);
            return Get(skey);
        }

        protected virtual IEnumerable<byte[]> Get(byte[] indexKey)
        {
            using (var iterator = Database.NewIterator())
            {
                var it = iterator.Seek(indexKey);
                while (it.Valid())
                {
                    if (!it.Key().ContainsSequence32(indexKey)) break;

                    var key = it.Value();
                    var value = Database.Get(key);
                    if (value == null || value.Length == 0)
                        continue;
                    yield return value;

                    it = it.Next();
                }
            }
        }


        public ISecondaryRepositoryTransaction CreateTransaction()
        {
            return new RocksRepositoryTransaction(this);
        }

        //public virtual Task<byte[][]> Get(ByteArray userId, OrderType orderType, int skip, int take)
        //{
        //    var skey = indexGenerator.GetParentOrderKey(userId, orderType);
        //    return Get(skey, skip, take);
        //}

        //public virtual Task<byte[][]> Get(OrderType orderType, int skip, int take)
        //{
        //    var skey = indexGenerator.GetEntityOrderKey(orderType);
        //    return Get(skey, skip, take);
        //}

        //protected virtual async Task<byte[][]> Get(byte[] skey, int skip, int take)
        //{

        //}
    }

    public interface IIndexOrderProvider
    {
        void SetOrder(Type type, IndexLevel indexLevel, OrderType orderType);
        OrderType GetOrder(Type type, IndexLevel indexType);
    }

    public class IndexOrderProvider : IIndexOrderProvider
    {
        private readonly Dictionary<Type, OrderType[]> indexOrders = new Dictionary<Type, OrderType[]>();

        public void SetOrder(Type type, IndexLevel indexLevel, OrderType orderType)
        {
            if (indexOrders.TryGetValue(type, out var orders))
                orders[(byte)indexLevel] = orderType;
            else
            {
                orders = new OrderType[256];
                orders[(byte)indexLevel] = orderType;
                indexOrders[type] = orders;
            }
        }

        public OrderType GetOrder(Type type, IndexLevel indexLevel)
        {
            if (indexOrders.TryGetValue(type, out var orders))
                return orders[(byte)indexLevel];

            return OrderType.None;
            //throw new KeyNotFoundException($"Index order for type '{type.Name}' and level '{indexLevel}' not found");
        }
    }

    public interface IIndex
    {
        IndexLevel Level { get; }
    }

    public class Index : IIndex
    {
        public IndexLevel Level { get; set; }
    }

    public interface IOrderIndex
    {
        long GetScore();
    }

    public class ScoreIndex : Index, IOrderIndex
    {
        public long Score { get; set; }

        public virtual long GetScore()
        {
            return Score;
        }
    }

    public class DateIndex : Index, IOrderIndex
    {
        public DateTime Date { get; set; }

        public virtual long GetScore()
        {
            var offset = new DateTimeOffset(Date);
            var score = offset.ToUnixTimeMilliseconds();
            return score;
        }
    }
}
