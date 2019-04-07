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

        public virtual byte[] GetParentEntityKey(ByteArray parentId, ByteArray entityId)
        {
            return GetParentEntityKey(parentId, entityId.Bytes);
        }

        public virtual byte[] GetParentEntityKey(ByteArray parentId, byte[] entityId)
        {
            var key = new byte[prefix.Length + entityName.Length + parentId.Bytes.Length + entityId.Length + 3];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Entity;
            key[index++] = 0;
            key[index++] = 0;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(parentId, ref index);
            key.CopyFrom(entityId, ref index);
            return key;
        }

        public virtual byte[] GetParentOrderKey(ByteArray parentId, OrderType order)
        {
            var key = new byte[prefix.Length + entityName.Length + parentId.Bytes.Length + 3];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Index;
            key[index++] = (byte)IndexLevel.Parent;
            key[index++] = (byte)order;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(parentId, ref index);
            return key;
        }

        public virtual byte[] GetEntityOrderKey(OrderType order)
        {
            var key = new byte[prefix.Length + entityName.Length + 3];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Index;
            key[index++] = (byte)IndexLevel.Parent;
            key[index++] = (byte)order;
            key.CopyFrom(entityName, ref index);
            return key;
        }

        //public byte[] GetOrderKey(ByteArray userId, ByteArray entityId, IndexLevel orderIndexType, long score, OrderType order)
        //{
        //    var orderBytes = GetOrderBytes(score, order);

        //    switch (orderIndexType)
        //    {
        //        case IndexLevel.SaveDate: return GetEntityOrderKey(userId, entityId, orderIndexType, orderBytes, order);
        //        case IndexLevel.EntityScore: return GetUserOrderKey(userId, entityId, orderIndexType, orderBytes, order);
        //        case IndexLevel.EntityDate: return GetUserOrderKey(userId, entityId, orderIndexType, orderBytes, order);
        //        default: throw new KeyNotFoundException($"Key for order index type '{orderIndexType}' not found");
        //    }

        //}

        //protected virtual byte[] GetUserOrderKey(ByteArray userId, ByteArray entityId, IndexLevel orderIndexType, long score, OrderType order)
        //{
        //    var orderBytes = GetOrderBytes(score, order);
        //    return GetUserOrderKey(userId, entityId, orderIndexType, orderBytes, order);
        //}

        //protected virtual byte[] GetUserOrderKey(ByteArray userId, ByteArray entityId, IndexLevel orderIndexType, DateTime date, OrderType order)
        //{
        //    var orderBytes = GetOrderBytes(date, order);
        //    return GetUserOrderKey(userId, entityId, orderIndexType, orderBytes, order);
        //}

        //protected virtual byte[] GetUserOrderKey(ByteArray userId, ByteArray entityId, IndexLevel orderIndexType, byte[] orderBytes, OrderType order)
        //{
        //    var key = new byte[prefix.Length + entityName.Length + userId.Bytes.Length + orderBytes.Length + entityId.Length + 1];

        //    var index = 0;
        //    key.CopyFrom(prefix, ref index);
        //    key[index++] = (byte)DataType.Index;
        //    key[index++] = (byte)orderIndexType;
        //    key[index++] = (byte)order;
        //    key.CopyFrom(entityName, ref index);
        //    key.CopyFrom(userId, ref index);
        //    key.CopyFrom(orderBytes, ref index);
        //    key.CopyFrom(entityId, ref index);
        //    return key;
        //}
        //protected virtual byte[] GetEntityOrderKey(ByteArray userId, ByteArray entityId, IndexLevel orderIndexType, DateTime date, OrderType order)
        //{
        //    var orderBytes = GetOrderBytes(date, order);
        //    return GetEntityOrderKey(userId, entityId, orderIndexType, orderBytes, order);
        //}

        public byte[] GetOrderKey(ByteArray parentId, ByteArray entityId, IndexLevel indexLevel, long score, OrderType order)
        {
            var orderBytes = GetOrderBytes(score, order);
            return GetOrderKey(parentId, entityId, indexLevel, orderBytes, order);
        }

        protected virtual byte[] GetOrderKey(ByteArray parentId, ByteArray entityId, IndexLevel indexLevel, byte[] orderBytes, OrderType order)
        {
            var key = new byte[prefix.Length + entityName.Length + orderBytes.Length + parentId.Length + entityId.Length + 2];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Index;
            key[index++] = (byte)indexLevel;
            key[index++] = (byte)order;
            key.CopyFrom(entityName, ref index);

            if (indexLevel == IndexLevel.Entity)
            {
                key.CopyFrom(orderBytes, ref index);
                key.CopyFrom(parentId, ref index);
            }
            else if (indexLevel == IndexLevel.Parent)
            {
                key.CopyFrom(parentId, ref index);
                key.CopyFrom(orderBytes, ref index);
            }
            else
                throw new NotSupportedException($"{nameof(IndexLevel)} '{indexLevel}' is not supported");

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
