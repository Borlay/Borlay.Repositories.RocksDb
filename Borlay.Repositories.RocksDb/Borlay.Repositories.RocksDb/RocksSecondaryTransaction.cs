using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public class RocksSecondaryTransaction : ISecondaryTransaction
    {
        protected readonly WriteBatch batch;
        protected readonly RocksSecondaryRepository repository;

        public RocksSecondaryTransaction(RocksSecondaryRepository repository)
        {
            this.batch = new WriteBatch();
            this.repository = repository;
        }

        public byte[] AppendValue(ByteArray userId, ByteArray entityId, byte[] value, int valueLength)
        {
            var key = repository.IndexGenerator.GetParentEntityKey(userId, entityId);
            batch.Put(key, (ulong)key.Length, value, (ulong)valueLength);
            return key;
        }

        public void AppendIndexes(ByteArray parentId, ByteArray entityId, byte[] key, params Index[] indexes)
        {
            foreach (var index in indexes)
            {
                var indexLevel = index.Level;
                var order = repository.IndexOrders.GetOrder(index.GetType(), indexLevel);

                if (index is IOrderIndex orderIndex)
                {
                    var score = orderIndex.GetScore();
                    AppendOrderIndex(parentId, entityId, key, indexLevel, score, order);
                }
            }
        }

        public void AppendOrderIndex(ByteArray parentId, ByteArray entityId, byte[] key, IndexLevel indexLevel, long score, OrderType orders)
        {
            if (orders.HasFlag(OrderType.Asc))
            {
                var orderKey = repository.IndexGenerator.GetOrderKey(parentId, entityId, indexLevel, score, OrderType.Asc);
                batch.Put(orderKey, key);
            }

            if (orders.HasFlag(OrderType.Desc))
            {
                var orderKey = repository.IndexGenerator.GetOrderKey(parentId, entityId, indexLevel, score, OrderType.Desc);
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

        public void Remove(ByteArray parentId, ByteArray entityId)
        {
            var key = repository.IndexGenerator.GetParentEntityKey(parentId, entityId);
            batch.Delete(key);
        }

        public void Remove(ByteArray parentId, ByteArray[] entityIds)
        {
            foreach (var entityId in entityIds)
                Remove(parentId, entityId);
        }
    }

    
}
