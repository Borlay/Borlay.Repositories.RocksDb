using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public class RocksRepositoryTransaction : IRepositoryTransaction
    {
        protected readonly WriteBatch batch;
        protected readonly RocksRepository repository;

        public RocksRepositoryTransaction(RocksRepository repository)
        {
            this.batch = new WriteBatch();
            this.repository = repository;
        }

        public async Task Append(ByteArray userId, ByteArray entityId, byte[] value, int valueLength, params OrderIndex[] indexes)
        {
            var key = repository.indexGenerator.GetUserEntityKey(userId, entityId, DataType.Entity);
            batch.Put(key, (ulong)key.Length, value, (ulong)valueLength);

            if (indexes != null)
                AppendIndexes(userId, entityId, key, indexes);
        }

        public void AppendIndexes(ByteArray userId, ByteArray entityId, byte[] key, params OrderIndex[] indexes)
        {
            foreach (var index in indexes)
            {
                var type = index.Type;
                var order = repository.GetOrder(type);
                long? score = null;

                if (index is IScoreIndex scoreIndex)
                    score = scoreIndex.Score;

                if (index is IDateIndex dateIndex)
                {
                    var offset = new DateTimeOffset(dateIndex.Date);
                    score = offset.ToUnixTimeMilliseconds();
                }

                if (!score.HasValue && index.Type == OrderIndexType.SaveDate)
                {
                    var offset = new DateTimeOffset(DateTime.Now);
                    score = offset.ToUnixTimeMilliseconds();
                }

                if (!score.HasValue)
                    throw new ArgumentException($"Bad index score");

                if (order.HasFlag(OrderType.Asc))
                {
                    var orderKey = repository.indexGenerator.GetOrderKey(userId, entityId, type, score.Value, OrderType.Asc);
                    batch.Put(orderKey, key);
                }

                if (order.HasFlag(Order.Desc))
                {
                    var orderKey = repository.indexGenerator.GetOrderKey(userId, entityId, type, score.Value, OrderType.Desc);
                    batch.Put(orderKey, key);
                }
            }
        }

        public void Commit()
        {
            repository.db.Write(batch, repository.writeOptions);
        }

        public void Dispose()
        {
            batch.Dispose();
        }
    }

    public interface IRepositoryTransaction : IDisposable
    {
        Task Append(ByteArray firstId, ByteArray secondId, byte[] value, int valueLength, params OrderIndex[] indexes);

        void Commit();
    }
}
