using Borlay.Arrays;
using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Repositories.RocksDb
{
    public class SecondaryIndexGenerator : IndexGenerator
    {
        public SecondaryIndexGenerator(string entityName)
            : base("secondary", entityName)
        {
        }

        public SecondaryIndexGenerator(string prefix, string entityName)
            : base(prefix, entityName)
        {
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

        public byte[] GetOrderKey(ByteArray parentId, ByteArray entityId, IndexLevel indexLevel, long score, OrderType order)
        {
            var orderBytes = GetOrderBytes(score, order);
            return GetOrderKey(parentId, entityId, indexLevel, orderBytes, order);
        }

        protected virtual byte[] GetOrderKey(ByteArray parentId, ByteArray entityId, IndexLevel indexLevel, byte[] orderBytes, OrderType order)
        {
            var key = new byte[prefix.Length + entityName.Length + orderBytes.Length + parentId.Length + entityId.Length + 3];

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
    }
}
