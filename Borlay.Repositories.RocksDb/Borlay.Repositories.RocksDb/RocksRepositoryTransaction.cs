using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public class RocksRepositoryTransaction : ISecondaryRepositoryTransaction
    {
        protected readonly WriteBatch batch;
        protected readonly RocksRepository repository;

        public RocksRepositoryTransaction(RocksRepository repository)
        {
            this.batch = new WriteBatch();
            this.repository = repository;
        }

        public async Task<byte[]> AppendValue(ByteArray userId, ByteArray entityId, byte[] value, int valueLength)
        {
            var key = repository.IndexGenerator.GetParentEntityKey(userId, entityId);
            batch.Put(key, (ulong)key.Length, value, (ulong)valueLength);
            return key;
        }

        public async Task AppendIndexes(ByteArray parentId, ByteArray entityId, byte[] key, params Index[] indexes)
        {
            foreach (var index in indexes)
            {
                var indexLevel = index.Level;
                var order = repository.IndexOrders.GetOrder(index.GetType(), indexLevel);

                if (index is IOrderIndex orderIndex)
                {
                    var score = orderIndex.GetScore();
                    await AppendOrderIndex(parentId, entityId, key, indexLevel, score, order);
                }
            }
        }

        public async Task AppendOrderIndex(ByteArray parentId, ByteArray entityId, byte[] key, IndexLevel indexLevel, long score, OrderType orders)
        {
            if (orders.HasFlag(OrderType.Asc))
            {
                var orderKey = repository.IndexGenerator.GetOrderKey(parentId, entityId, indexLevel, score, OrderType.Asc);
                batch.Put(orderKey, key);
            }

            if (orders.HasFlag(Order.Desc))
            {
                var orderKey = repository.IndexGenerator.GetOrderKey(parentId, entityId, indexLevel, score, OrderType.Desc);
                batch.Put(orderKey, key);
            }
        }

        public void Commit()
        {
            repository.Database.Write(batch, repository.WriteOptions);
        }

        public void Dispose()
        {
            batch.Dispose();
        }
    }

    public interface ICommit
    {
        void Commit();
    }

    public interface ITransaction : ICommit, IDisposable
    {
    }

    public interface ISecondaryRepositoryTransaction : ITransaction, IDisposable
    {
        Task<byte[]> AppendValue(ByteArray parentId, ByteArray entityId, byte[] value, int valueLength);

        Task AppendIndexes(ByteArray parentId, ByteArray entityId, byte[] key, params Index[] indexes);

        Task AppendOrderIndex(ByteArray parentId, ByteArray entityId, byte[] key, IndexLevel indexLevel, long score, OrderType orders);

        void Commit();
    }
}
