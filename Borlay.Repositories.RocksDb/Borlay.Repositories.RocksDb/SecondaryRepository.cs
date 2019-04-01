using Borlay.Arrays;
using Borlay.Injection;
using Borlay.Serialization.Converters;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public class SecondaryRepository<T> : ISecondaryRepository<T> where T : IEntity
    {
        private readonly ISerializer serializer;
        private readonly RocksDbSharp.RocksDb db;

        private static byte[] entityName = Encoding.UTF8.GetBytes(typeof(T).Name);
        private static byte[] userName = Encoding.UTF8.GetBytes("user");

        public SecondaryRepository(RocksDbSharp.RocksDb rocksDb, Serializer serializer)
        {

            this.db = rocksDb;
            this.serializer = serializer;
        }

        public async Task<T> Get(ByteArray userId, ByteArray entityId)
        {
            var key = GetKey(userId, entityId);
            var value = db.Get(key);

            var index = 0;
            var entity = serializer.GetObject(value, ref index);
            return (T)entity;
        }

        public async Task<T[]> Get(ByteArray userId, int skip, int take)
        {
            var key = GetKey(userId);
            using (var iterator = db.NewIterator())
            {
                List<T> list = new List<T>();

                var it = iterator.Seek(key);
                while (it.Valid() && take != 0)
                {
                    if (skip <= 0)
                    {
                        var value = it.Value();
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

        public async Task Save(ByteArray userId, T entity)
        {
            var key = GetKey(userId, entity.Id);

            var value = new byte[512];
            var index = 0;
            serializer.AddBytes(entity, value, ref index);

            db.Put(key, key.Length, value, index);
        }

        protected byte[] GetKey(ByteArray userId)
        {
            var key = new byte[userName.Length + userId.Bytes.Length + entityName.Length];

            var index = 0;
            key.CopyFrom(userName, ref index);
            key.CopyFrom(userId, ref index);
            key.CopyFrom(entityName, ref index);
            return key;
        }

        protected byte[] GetKey(ByteArray userId, ByteArray entityId)
        {
            //DateTimeOffset.Now.ToUnixTimeMilliseconds();

            var key = new byte[userName.Length + userId.Bytes.Length + entityName.Length + entityId.Bytes.Length];

            var index = 0;
            key.CopyFrom(userName, ref index);
            key.CopyFrom(userId, ref index);
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(entityId, ref index);
            return key;
        }
    }
}
