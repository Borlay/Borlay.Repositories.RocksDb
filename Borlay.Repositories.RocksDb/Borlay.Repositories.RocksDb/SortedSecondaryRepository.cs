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
        public OrderType SaveOrder { get; set; } = OrderType.Desc;
        public OrderType ScoreOrder { get; set; } = OrderType.Desc;
        public OrderType DateOrder { get; set; } = OrderType.Desc;

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
            var key = GetUserEntityKey(userId, entity.Id, 0);
            var value = new byte[BufferSize];
            var index = 0;
            serializer.AddBytes(entity, value, ref index);
            batch.Put(key, (ulong)key.Length, value, (ulong)index);


            if (entity is IScoreEntity scoreEntity)
            {
                var score = scoreEntity.Score;
                if (ScoreOrder.HasFlag(OrderType.Asc))
                {
                    var skey = GetUserOrderKey(userId, entity.Id, DataType.Score, score, OrderType.Asc);
                    batch.Put(skey, key);
                }

                if (ScoreOrder.HasFlag(OrderType.Desc))
                {
                    var skey = GetUserOrderKey(userId, entity.Id, DataType.Score, score, OrderType.Desc);
                    batch.Put(skey, key);
                }
            }

            if (entity is IDateEntity dateEntity)
            {
                var score = dateEntity.Date;
                if (DateOrder.HasFlag(OrderType.Asc))
                {
                    var skey = GetUserOrderKey(userId, entity.Id, DataType.Date, score, OrderType.Asc); // todo dont use GetDateKey, see bellow
                    batch.Put(skey, key);
                }

                if (DateOrder.HasFlag(OrderType.Desc))
                {
                    var skey = GetUserOrderKey(userId, entity.Id, DataType.Date, score, OrderType.Desc);
                    batch.Put(skey, key);
                }
            }


            var date = DateTime.Now;

            if (SaveOrder.HasFlag(OrderType.Asc))
            {
                var dateKey = GetEntityOrderKey(userId, entity.Id, DataType.Save, date, OrderType.Asc);
                batch.Put(dateKey, key);
            }

            if (SaveOrder.HasFlag(OrderType.Desc))
            {
                var dateKey = GetEntityOrderKey(userId, entity.Id, DataType.Save, date, OrderType.Desc);
                batch.Put(dateKey, key);
            }
        }

        public virtual Task<T[]> Get(ByteArray userId, int skip, int take)
        {
            var skey = GetUserKey(userId, DataType.Save);
            return Get((byte[])skey, (byte[] entityIdBytes) => base.GetUserEntityKey(userId, entityIdBytes, 0), skip, take);
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
