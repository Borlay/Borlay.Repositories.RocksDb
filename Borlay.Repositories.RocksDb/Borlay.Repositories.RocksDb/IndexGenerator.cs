using Borlay.Arrays;
using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Repositories.RocksDb
{
    public class IndexGenerator
    {
        public byte[] entityName { get; } // = Encoding.UTF8.GetBytes($"entity:{typeof(T).Name}:");
        public byte[] prefix { get; } // = Encoding.UTF8.GetBytes("secondary:");

        public IndexGenerator(string prefix, string entityName)
        {
            this.entityName = Encoding.UTF8.GetBytes($"entity:{entityName}:");
            this.prefix = Encoding.UTF8.GetBytes($"{prefix}:");
        }

        public virtual byte[] GetEntityOrderKey(OrderType order)
        {
            var key = new byte[prefix.Length + entityName.Length + 3];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Index;
            key[index++] = (byte)IndexLevel.Entity;
            key[index++] = (byte)order;
            key.CopyFrom(entityName, ref index);
            return key;
        }

        protected virtual byte[] GetOrderBytes(DateTime date, OrderType order)
        {
            var offset = new DateTimeOffset(date);
            var score = offset.ToUnixTimeMilliseconds();
            var orderBytes = GetOrderBytes(score, order);
            return orderBytes;
        }

        protected virtual byte[] GetOrderBytes(long score, OrderType order)
        {
            if (order == OrderType.Desc)
                score = long.MaxValue - score;

            var scoreBytes = BitConverter.GetBytes(score);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(scoreBytes);

            return scoreBytes;
        }
    }
}
