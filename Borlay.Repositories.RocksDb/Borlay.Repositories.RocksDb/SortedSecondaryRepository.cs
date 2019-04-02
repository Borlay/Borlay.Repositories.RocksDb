using Borlay.Arrays;
using Borlay.Serialization.Converters;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public class SortedSecondaryRepository<T> : SecondaryRepositoryBase<T>, ISortedSecondaryRepository<T> where T : IEntity
    {
        public Order Order { get; set; } = Order.Desc;

        public bool AllowOrderDublicates { get; set; } = true;
        public bool SkipDublicates { get; set; } = true;

        protected int index = 0;

        public SortedSecondaryRepository(RocksDbSharp.RocksDb rocksDb, Serializer serializer)
            : base(rocksDb, serializer)
        {
        }

        public async Task Save(ByteArray userId, T entity)
        {
            var score = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            if (entity is IScoreEntity scoreEntity)
                score = scoreEntity.Score;

            var scoreBytes = GetScoreBytes(score);

            var key = GetKey(userId, entity.Id, 0);
            var skey = GetScoreKey(userId, entity.Id, scoreBytes);
            var scoreKey = GetKey(userId, entity.Id, 2);

            var value = new byte[BufferSize];
            var index = 0;
            serializer.AddBytes(entity, value, ref index);

            WriteBatch batch = new WriteBatch();

            batch.Put(key, (ulong)key.Length, value, (ulong)index);
            

            bool addSKey = true;
            if(!AllowOrderDublicates)
            {
                var existScoreBytes = db.Get(scoreKey);
                if(existScoreBytes != null && existScoreBytes.Length > 0)
                {
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(existScoreBytes);

                    var existScore = BitConverter.ToInt64(existScoreBytes, 0);

                    if (Order == Order.Desc)
                        existScore = long.MaxValue - existScore;

                    if (existScore != score)
                    {
                        if (BitConverter.IsLittleEndian)
                            Array.Reverse(existScoreBytes);

                        var existskey = GetScoreKey(userId, entity.Id, existScoreBytes);
                        batch.Delete(existskey);
                    }
                    else
                        addSKey = false;
                }
            }

            if(addSKey)
            {
                batch.Put(skey, entity.Id.Bytes);
                batch.Put(scoreKey, scoreBytes);
            }

            db.Write(batch);
        }

        public virtual async Task<T[]> Get(ByteArray userId, int skip, int take)
        {
            var skey = GetKey(userId, 1);
            using (var iterator = db.NewIterator())
            {
                List<T> list = new List<T>();

                var it = iterator.Seek(skey);
                while (it.Valid() && take != 0)
                {
                    if (!it.Key().ContainsSequence32(skey)) break;

                    if (skip <= 0)
                    {
                        var entityIdBytes = it.Value();

                        var key = GetKey(userId, entityIdBytes, 0);
                        var value = db.Get(key);

                        var index = 0;
                        var entity = serializer.GetObject(value, ref index);
                        list.Add((T)entity);
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

        protected virtual byte[] GetKey(ByteArray userId, byte bit)
        {
            var key = new byte[userName.Length + userId.Bytes.Length + entityName.Length + 1];

            var index = 0;
            key.CopyFrom(userName, ref index);
            key.CopyFrom(userId, ref index);
            key[index++] = bit;
            key.CopyFrom(entityName, ref index);
            return key;
        }

        protected virtual byte[] GetScoreBytes(long score)
        {
            if (Order == Order.Desc)
                score = long.MaxValue - score;

            var scoreBytes = BitConverter.GetBytes(score);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(scoreBytes);

            return scoreBytes;
        }

        protected virtual byte[] GetScoreKey(ByteArray userId, ByteArray entityId, long score)
        {
            var scoreBytes = GetScoreBytes(score);
            return GetScoreKey(userId, entityId, scoreBytes);
        }

        protected virtual byte[] GetScoreKey(ByteArray userId, ByteArray entityId, byte[] scoreBytes)
        {
            var key = new byte[userName.Length + userId.Bytes.Length + entityName.Length + scoreBytes.Length + entityId.Length + 1];

            var index = 0;
            key.CopyFrom(userName, ref index);
            key.CopyFrom(userId, ref index);
            key[index++] = 1;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(scoreBytes, ref index);
            key.CopyFrom(entityId, ref index);
            return key;
        }
    }
}
