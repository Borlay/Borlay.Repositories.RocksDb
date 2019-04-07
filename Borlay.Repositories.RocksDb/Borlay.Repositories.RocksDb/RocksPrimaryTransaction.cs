using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public class RocksPrimaryTransaction : IPrimaryTransaction
    {
        protected readonly WriteBatch batch;
        protected readonly RocksPrimaryRepository repository;

        public RocksPrimaryTransaction(RocksPrimaryRepository repository)
        {
            this.batch = new WriteBatch();
            this.repository = repository;
        }

        public byte[] AppendValue(ByteArray entityId, byte[] value, int valueLength)
        {
            var key = repository.IndexGenerator.GetEntityKey(entityId);
            batch.Put(key, (ulong)key.Length, value, (ulong)valueLength);
            return key;
        }

        public void AppendIndexes(ByteArray entityId, byte[] key, params Index[] indexes)
        {
            foreach (var index in indexes)
            {
                var indexLevel = index.Level;
                var order = repository.IndexOrders.GetOrder(index.GetType(), indexLevel);

                if (index is IOrderIndex orderIndex)
                {
                    var score = orderIndex.GetScore();
                    AppendOrderIndex(entityId, key, indexLevel, score, order);
                }
            }
        }

        public void AppendOrderIndex(ByteArray entityId, byte[] key, IndexLevel indexLevel, long score, OrderType orders)
        {
            if (orders.HasFlag(OrderType.Asc))
            {
                var orderKey = repository.IndexGenerator.GetOrderKey(entityId, score, OrderType.Asc);
                batch.Put(orderKey, key);
            }

            if (orders.HasFlag(OrderType.Desc))
            {
                var orderKey = repository.IndexGenerator.GetOrderKey(entityId, score, OrderType.Desc);
                batch.Put(orderKey, key);
            }
        }

        public async Task Commit()
        {
            repository.Database.Write(batch, repository.WriteOptions);
        }

        public void Dispose()
        {
            batch.Dispose();
        }
    }
}
