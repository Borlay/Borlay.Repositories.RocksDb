using Borlay.Arrays;
using Borlay.Serialization.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public abstract class SecondaryRepositoryBase<T> where T : IEntity
    {
        public static int BufferSize { get; set; } = 512;

        public bool AddInsertDate { get; set; } = true;

        protected readonly ISerializer serializer;
        protected readonly RocksDbSharp.RocksDb db;

        public static byte[] entityName { get; set; } = Encoding.UTF8.GetBytes($"entity:{typeof(T).Name}:");
        public static byte[] dataName { get; set; } = Encoding.UTF8.GetBytes("data:secondary:");
        public static byte[] insertName { get; set; } = Encoding.UTF8.GetBytes("operation:");

        public SecondaryRepositoryBase(RocksDbSharp.RocksDb rocksDb, Serializer serializer)
        {
            this.db = rocksDb;
            this.serializer = serializer;
        }

        public virtual async Task<T> Get(ByteArray userId, ByteArray entityId)
        {
            var key = GetKey(userId, entityId, 0);
            var value = db.Get(key);

            var index = 0;
            var entity = serializer.GetObject(value, ref index);
            return (T)entity;
        }

        public virtual async Task<T[]> Get(ByteArray userId, ByteArray[] entityIds)
        {
            var keys = entityIds.Select(id => GetKey(userId, id, 0)).ToArray();
            var values = db.MultiGet(keys);

            var result = new T[values.Length];

            var index = 0;
            for(int i = 0; i < result.Length; i++)
            {
                index = 0;
                var entity = serializer.GetObject(values[i].Value, ref index);
                result[i] = (T)entity;
            }

            return result;
        }

        protected virtual byte[] GetKey(ByteArray userId, ByteArray entityId, byte bit)
        {
            return GetKey(userId, entityId.Bytes, bit);
        }

        protected virtual byte[] GetKey(ByteArray userId, byte[] entityId, byte bit)
        {
            var key = new byte[dataName.Length + userId.Bytes.Length + entityName.Length + entityId.Length + 1];

            var index = 0;
            key.CopyFrom(dataName, ref index);
            key[index++] = bit; // todo change to enum
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(userId, ref index);
            key.CopyFrom(entityId, ref index);
            return key;
        }

        protected virtual byte[] GetKey(ByteArray userId, byte bit)
        {
            var key = new byte[dataName.Length + userId.Bytes.Length + entityName.Length + 1];

            var index = 0;
            key.CopyFrom(dataName, ref index);
            key[index++] = bit;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(userId, ref index);
            return key;
        }

        protected virtual byte[] GetScoreKey(ByteArray userId, ByteArray entityId, long score, Order order)
        {
            var scoreBytes = GetScoreBytes(score, order);
            return GetScoreKey(userId, entityId, scoreBytes);
        }

        protected virtual byte[] GetScoreKey(ByteArray userId, ByteArray entityId, byte[] scoreBytes)
        {
            var key = new byte[dataName.Length + userId.Bytes.Length + entityName.Length + scoreBytes.Length + entityId.Length + 1];

            var index = 0;
            key.CopyFrom(dataName, ref index);
            key[index++] = 1;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(userId, ref index);
            key.CopyFrom(scoreBytes, ref index);
            key.CopyFrom(entityId, ref index);
            return key;
        }

        protected virtual byte[] GetDateKey(ByteArray userId, ByteArray entityId, DateTime date, Order order)
        {
            var offset = new DateTimeOffset(date);
            var score = offset.ToUnixTimeMilliseconds();
            var scoreBytes = GetScoreBytes(score, order);

            var key = new byte[insertName.Length + entityName.Length + entityId.Length + userId.Length + 8 + 1];

            var index = 0;
            key.CopyFrom(insertName, ref index);
            key[index++] = 1;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(scoreBytes, ref index);
            key.CopyFrom(userId, ref index);
            key.CopyFrom(entityId, ref index);
            
            return key;
        }

        protected virtual byte[] GetScoreBytes(long score, Order order)
        {
            if (order == Order.Desc)
                score = long.MaxValue - score;

            var scoreBytes = BitConverter.GetBytes(score);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(scoreBytes);

            return scoreBytes;
        }
    }

    [Flags]
    public enum Order
    {
        None = 0,
        Asc = 1,
        Desc = 2
    }
}
