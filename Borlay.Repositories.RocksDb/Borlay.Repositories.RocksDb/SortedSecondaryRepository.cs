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
        public Order SaveOrder { get; set; } = Order.Desc;
        public Order ScoreOrder { get; set; } = Order.Desc;
        public Order DateOrder { get; set; } = Order.Desc;

        protected readonly WriteOptions writeOptions;

        public SortedSecondaryRepository(RocksDbSharp.RocksDb rocksDb, Serializer serializer)
            : base(rocksDb, serializer)
        {
            writeOptions = new WriteOptions();
            writeOptions.SetSync(true);
            
        }

        public async Task Save(ByteArray userId, T entity)
        {
            WriteBatch batch = new WriteBatch();

            Append(batch, userId, entity);

            db.Write(batch, writeOptions);
        }

        public async Task Save(ByteArray userId, T[] entities)
        {
            WriteBatch batch = new WriteBatch();
            
            foreach(var entity in entities)
            {
                Append(batch, userId, entity);
            }

            db.Write(batch, writeOptions);
        }

        public void Append(WriteBatch batch, ByteArray userId, T entity)
        {
            var key = GetKey(userId, entity.Id, 0);
            var value = new byte[BufferSize];
            var index = 0;
            serializer.AddBytes(entity, value, ref index);
            batch.Put(key, (ulong)key.Length, value, (ulong)index);


            if (entity is IScoreEntity scoreEntity)
            {
                var score = scoreEntity.Score;
                if (ScoreOrder.HasFlag(Order.Asc))
                {
                    var skey = GetScoreKey(userId, entity.Id, score, Order.Asc);
                    batch.Put(skey, entity.Id.Bytes);
                }

                if (ScoreOrder.HasFlag(Order.Desc))
                {
                    var skey = GetScoreKey(userId, entity.Id, score, Order.Desc);
                    batch.Put(skey, entity.Id.Bytes);
                }
            }

            if (entity is IDateEntity dateEntity)
            {
                var score = dateEntity.Date;
                if (DateOrder.HasFlag(Order.Asc))
                {
                    var skey = GetDateKey(userId, entity.Id, score, Order.Asc); // todo dont use GetDateKey, see bellow
                    batch.Put(skey, entity.Id.Bytes);
                }

                if (DateOrder.HasFlag(Order.Desc))
                {
                    var skey = GetDateKey(userId, entity.Id, score, Order.Desc);
                    batch.Put(skey, entity.Id.Bytes);
                }
            }


            var date = DateTime.Now;

            if (SaveOrder.HasFlag(Order.Asc))
            {
                var dateKey = GetDateKey(userId, entity.Id, date, Order.Asc);
                batch.Put(dateKey, key);
            }

            if (SaveOrder.HasFlag(Order.Desc))
            {
                var dateKey = GetDateKey(userId, entity.Id, date, Order.Desc);
                batch.Put(dateKey, key);
            }
        }

        public virtual Task<T[]> Get(ByteArray userId, int skip, int take)
        {
            var skey = GetKey(userId, 1);
            return Get(skey, entityIdBytes => GetKey(userId, entityIdBytes, 0), skip, take);
        }

        //public virtual Task<T[]> Get(int skip, int take)
        //{
        //    var skey = GetKey(userId, 1);
        //    return Get(skey, entityIdBytes => GetKey(userId, entityIdBytes, 0), skip, take);
        //}

        protected virtual async Task<T[]> Get(byte[] skey, Func<byte[], byte[]> resolveKey, int skip, int take)
        {
            //var skey = GetKey(userId, 1);
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

                        //var key = GetKey(userId, entityIdBytes, 0);
                        var key = resolveKey(entityIdBytes);
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

        
    }





    //bool addSKey = true;
    //if (!AllowOrderDublicates)
    //{
    //    var existScoreBytes = db.Get(scoreKey);
    //    if (existScoreBytes != null && existScoreBytes.Length > 0)
    //    {
    //        if (BitConverter.IsLittleEndian)
    //            Array.Reverse(existScoreBytes);

    //        var existScore = BitConverter.ToInt64(existScoreBytes, 0);

    //        if (Order == Order.Desc)
    //            existScore = long.MaxValue - existScore;

    //        //replaced bellow
    //        //if (existScore == score)
    //        //    addSKey = false;

    //        if (existScore != score)
    //        {
    //            if (BitConverter.IsLittleEndian)
    //                Array.Reverse(existScoreBytes);

    //            var existskey = GetScoreKey(userId, entity.Id, existScoreBytes);
    //            var val = db.Get(existskey);

    //            batch.Delete(existskey);
    //        }
    //        else
    //            addSKey = false;
    //    }
    //}
}
