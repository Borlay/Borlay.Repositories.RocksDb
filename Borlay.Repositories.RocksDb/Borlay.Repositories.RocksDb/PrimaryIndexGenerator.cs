using Borlay.Arrays;
using System;
using System.Collections.Generic;
using System.Text;

namespace Borlay.Repositories.RocksDb
{
    public class PrimaryIndexGenerator : IndexGenerator
    {
        public PrimaryIndexGenerator(string entityName)
            : base("primary", entityName)
        {
        }

        public PrimaryIndexGenerator(string prefix, string entityName)
            : base(prefix, entityName)
        {
        }

        public virtual byte[] GetEntityKey(ByteArray entityId)
        {
            return GetEntityKey(entityId.Bytes);
        }

        public virtual byte[] GetEntityKey(byte[] entityId)
        {
            var key = new byte[prefix.Length + entityName.Length + entityId.Length + 3];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Entity;
            key[index++] = 0;
            key[index++] = 0;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(entityId, ref index);
            return key;
        }

        public byte[] GetOrderKey(ByteArray entityId, long score, OrderType order)
        {
            var orderBytes = GetOrderBytes(score, order);
            return GetOrderKey(entityId, orderBytes, order);
        }

        protected virtual byte[] GetOrderKey(ByteArray entityId, byte[] orderBytes, OrderType order)
        {
            var key = new byte[prefix.Length + entityName.Length + orderBytes.Length + entityId.Length + 3];

            var index = 0;
            key.CopyFrom(prefix, ref index);
            key[index++] = (byte)DataType.Index;
            key[index++] = (byte)IndexLevel.Entity;
            key[index++] = (byte)order;
            key.CopyFrom(entityName, ref index);
            key.CopyFrom(orderBytes, ref index);
            key.CopyFrom(entityId, ref index);

            return key;
        }
    }
}
