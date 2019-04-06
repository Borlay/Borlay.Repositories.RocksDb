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

        
        //public static byte[] operationName { get; set; } = Encoding.UTF8.GetBytes("operation:");

        public SecondaryRepositoryBase(RocksDbSharp.RocksDb rocksDb, Serializer serializer)
        {
            this.db = rocksDb;
            this.serializer = serializer;
        }

        public virtual async Task<T> Get(ByteArray userId, ByteArray entityId)
        {
            var key = GetUserEntityKey(userId, entityId, 0);
            var value = db.Get(key);

            var index = 0;
            var entity = serializer.GetObject(value, ref index);
            return (T)entity;
        }

        public virtual async Task<T[]> Get(ByteArray userId, ByteArray[] entityIds)
        {
            var keys = entityIds.Select(id => GetUserEntityKey(userId, id, 0)).ToArray();
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

        
    }

    public enum DataType : byte
    {
        None = 0,
        Entity = 1,
        Index = 2
    }

    public enum OrderIndexType : byte
    {
        None = 0,
        SaveDate = 1,
        EntityScore,
        EntityDate
    }

    [Flags]
    public enum OrderType
    {
        None = 0,
        Asc = 1,
        Desc = 2
    }
}
