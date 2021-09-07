using System.Numerics;
using System.Text;

namespace ChordDHT
{
    public record Node(string Host, int Port)
    {
        public BigInteger Id { get; } = NodeId.ComputeId(Host, Port);

        protected virtual bool PrintMembers(StringBuilder builder)
        {
            builder.AppendFormat($"Url = https://{Host}:{Port}, ");
            builder.AppendFormat($"Id = {NodeId.ToString(Id)}");
            return true;
        }
    }
}