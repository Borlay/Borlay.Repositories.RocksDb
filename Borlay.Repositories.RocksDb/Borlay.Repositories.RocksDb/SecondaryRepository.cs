using Borlay.Arrays;
using Borlay.Injection;
using Borlay.Serialization.Converters;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{
    public class SecondaryRepository<T> : SecondaryRepositoryBase<T>, ISecondaryRepository<T> where T : IEntity
    {
        public SecondaryRepository(RocksDbSharp.RocksDb rocksDb, Serializer serializer)
            : base(rocksDb, serializer)
        {
        }

        public virtual async Task Save(ByteArray userId, T entity)
        {
            var key = GetKey(userId, entity.Id, 0);

            var value = new byte[BufferSize];
            var index = 0;
            serializer.AddBytes(entity, value, ref index);

            db.Put(key, key.Length, value, index);
        }
    }
}
