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

        public virtual byte[] GetUserEntityKey(ByteArray userId, ByteArray entityId, DataType dataType)
        {
            return GetUserEntityKey(userId, entityId.Bytes, dataType);
        }

        public virtual byte[] GetUserEntityKey(ByteArray userId, byte[] entityId, DataType dataType)
        {
            var key = new byte[prefix.Length + userId.Bytes.Length + entityName.Length + entityId.Length + 1];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)dataType;
            key[index++] = 0;
            key[index++] = 0;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(userId, ref index);
            key.CopyFrom(entityId, ref index);
            return key;
        }

        public virtual byte[] GetOrderKey(ByteArray userId, OrderIndexType orderIndexType, OrderType order)
        {
            var key = new byte[prefix.Length + userId.Bytes.Length + entityName.Length + 1];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Index;
            key[index++] = (byte)orderIndexType;
            key[index++] = (byte)order; // different for SaveDate
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(userId, ref index);
            return key;
        }

        public byte[] GetOrderKey(ByteArray userId, ByteArray entityId, OrderIndexType orderIndexType, long score, OrderType order)
        {
            var orderBytes = GetOrderBytes(score, order);

            switch (orderIndexType)
            {
                case OrderIndexType.SaveDate: return GetEntityOrderKey(userId, entityId, orderIndexType, orderBytes, order);
                case OrderIndexType.EntityScore: return GetUserOrderKey(userId, entityId, orderIndexType, orderBytes, order);
                case OrderIndexType.EntityDate: return GetUserOrderKey(userId, entityId, orderIndexType, orderBytes, order);
                default: throw new KeyNotFoundException($"Key for order index type '{orderIndexType}' not found");
            }

        }

        protected virtual byte[] GetUserOrderKey(ByteArray userId, ByteArray entityId, OrderIndexType orderIndexType, long score, OrderType order)
        {
            var orderBytes = GetOrderBytes(score, order);
            return GetUserOrderKey(userId, entityId, orderIndexType, orderBytes, order);
        }

        protected virtual byte[] GetUserOrderKey(ByteArray userId, ByteArray entityId, OrderIndexType orderIndexType, DateTime date, OrderType order)
        {
            var orderBytes = GetOrderBytes(date, order);
            return GetUserOrderKey(userId, entityId, orderIndexType, orderBytes, order);
        }

        protected virtual byte[] GetUserOrderKey(ByteArray userId, ByteArray entityId, OrderIndexType orderIndexType, byte[] orderBytes, OrderType order)
        {
            var key = new byte[prefix.Length + entityName.Length + userId.Bytes.Length + orderBytes.Length + entityId.Length + 1];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Index;
            key[index++] = (byte)orderIndexType;
            key[index++] = (byte)order;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(userId, ref index);
            key.CopyFrom(orderBytes, ref index);
            key.CopyFrom(entityId, ref index);
            return key;
        }
        protected virtual byte[] GetEntityOrderKey(ByteArray userId, ByteArray entityId, OrderIndexType orderIndexType, DateTime date, OrderType order)
        {
            var orderBytes = GetOrderBytes(date, order);
            return GetEntityOrderKey(userId, entityId, orderIndexType, orderBytes, order);
        }

        protected virtual byte[] GetEntityOrderKey(ByteArray userId, ByteArray entityId, OrderIndexType orderIndexType, byte[] orderBytes, OrderType order)
        {
            var key = new byte[prefix.Length + entityName.Length + orderBytes.Length + userId.Length + entityId.Length + 2];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Index;
            key[index++] = (byte)orderIndexType;
            key[index++] = (byte)order;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(orderBytes, ref index);
            key.CopyFrom(userId, ref index);
            key.CopyFrom(entityId, ref index);

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
