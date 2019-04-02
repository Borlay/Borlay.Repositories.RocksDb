using Borlay.Arrays;
using Borlay.Serialization.Converters;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public abstract class SecondaryRepositoryBase<T> where T : IEntity
    {
        public static int BufferSize { get; set; } = 512;

        protected readonly ISerializer serializer;
        protected readonly RocksDbSharp.RocksDb db;

        public static byte[] entityName { get; set; } = Encoding.UTF8.GetBytes(typeof(T).Name);
        public static byte[] userName { get; set; } = Encoding.UTF8.GetBytes("secondary");

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

        protected virtual byte[] GetKey(ByteArray userId, ByteArray entityId, byte bit)
        {
            return GetKey(userId, entityId.Bytes, bit);
        }

        protected virtual byte[] GetKey(ByteArray userId, byte[] entityId, byte bit)
        {
            var key = new byte[userName.Length + userId.Bytes.Length + entityName.Length + entityId.Length + 1];

            var index = 0;
            key.CopyFrom(userName, ref index);
            key.CopyFrom(userId, ref index);
            key[index++] = bit;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(entityId, ref index);
            return key;
        }
    }

    public enum Order
    {
        Asc = 1,
        Desc = 2
    }
}
