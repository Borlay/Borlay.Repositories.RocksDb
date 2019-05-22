using Borlay.Arrays;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Repositories.RocksDb
{
    public static class RocksExtensions
    {
        public static IEnumerable<KeyValuePair<byte[], byte[]>> Search(this RocksDbSharp.RocksDb database, byte[] indexKey, bool distinct, bool isSubKey)
        {
            Dictionary<ByteArray, bool> dictionary = new Dictionary<ByteArray, bool>();

            using (var iterator = database.NewIterator())
            {
                var it = iterator.Seek(indexKey);
                while (it.Valid())
                {
                    var iterKey = it.Key();
                    if (!iterKey.ContainsSequence32(indexKey)) break;

                    if (isSubKey)
                    {
                        var key = it.Value();

                        if (key == null || key.Length == 0) continue;

                        if (distinct)
                        {
                            var bKey = new ByteArray(key);
                            if (dictionary.ContainsKey(bKey)) continue;
                            dictionary[bKey] = true;
                        }

                        var value = database.Get(key);
                        if (value == null || value.Length == 0) continue;

                        yield return new KeyValuePair<byte[], byte[]>(key, value);

                    }
                    else
                    {
                        if (distinct)
                        {
                            var bKey = new ByteArray(iterKey);
                            if (dictionary.ContainsKey(bKey)) continue;
                            dictionary[bKey] = true;
                        }

                        var value = it.Value();
                        if (value == null || value.Length == 0) continue;

                        yield return new KeyValuePair<byte[], byte[]>(iterKey, value);
                    }

                    it = it.Next();
                }
            }
        }
    }
}
