using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Repositories.RocksDb
{
    public static class RocksExtensions
    {
        public static IEnumerable<byte[]> Search(this RocksDbSharp.RocksDb database, byte[] indexKey)
        {
            using (var iterator = database.NewIterator())
            {
                var it = iterator.Seek(indexKey);
                while (it.Valid())
                {
                    if (!it.Key().ContainsSequence32(indexKey)) break;

                    var key = it.Value();
                    var value = database.Get(key);
                    if (value == null || value.Length == 0)
                        continue;
                    yield return value;

                    it = it.Next();
                }
            }
        }
    }
}
