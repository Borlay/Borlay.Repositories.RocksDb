using Borlay.Arrays;
using Borlay.Serialization.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Borlay.Repositories.RocksDb
{

    public enum DataType : byte
    {
        None = 0,
        Entity = 1,
        Index = 2
    }

    public enum IndexLevel : byte
    {
        None = 0,
        Entity = 1,
        Parent = 2,
    }

    [Flags]
    public enum OrderType
    {
        None = 0,
        Asc = 1,
        Desc = 2
    }
}
