namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;

    internal class TxIdList : List<ulong>
    {
        public override string ToString()
        {
            return this.Any() ? string.Join("|", this) : "No transaction ids";
        }
    }
}