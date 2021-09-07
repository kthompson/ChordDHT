using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;

namespace ChordDHT
{
    public static class NodeId
    {
        // SHA1 is 160 bits or 20 Bytes
        public const int Bits = 160; // SHA1
        public static readonly BigInteger MaxValue = FromBytes(new byte[20]
        {
            255, 255, 255, 255, 255,
            255, 255, 255, 255, 255,
            255, 255, 255, 255, 255,
            255, 255, 255, 255, 255,
        });

        public static BigInteger ComputeId(Node node)
        {
            var host = node.Host;
            var port = node.Port;

            return ComputeId(host, port);
        }

        public static BigInteger FromBytes(byte[] bytes) => new(bytes, true);
        public static BigInteger ComputeId(string? host, int port)
        {
            using var hash = SHA1.Create();
            using var stream = new CryptoStream(Stream.Null, hash, CryptoStreamMode.Write);

            var data = Encoding.Default.GetBytes($"{host}:{port}");

            stream.Write(data, 0, data.Length);
            stream.FlushFinalBlock();

            var digest = hash.Hash;
            if (digest == null)
                throw new InvalidOperationException("Failed to compute hash");

            return new BigInteger(digest, true);
        }

        public static byte[] IdToByteArray(string id)
        {
            var array = new byte[20];

            for (int i = 0; i < 40; i += 2)
            {
                array[i / 2] = byte.Parse(id.Substring(i, 2), NumberStyles.HexNumber);
            }

            return array;
        }

        public static string ToString(BigInteger integer) => ByteArrayToId(integer.ToByteArray());
        public static BigInteger FromString(string nodeId) => FromBytes(IdToByteArray(nodeId));

        private static string ByteArrayToId(IEnumerable<byte> sha)
        {
            var sb = new StringBuilder(40);

            foreach (var b in sha)
            {
                sb.Append(Convert.ToString(b, 16).PadLeft(2, '0'));
            }

            return sb.ToString();
        }



        /// <summary>
        /// Checks whether a key is in a specified range.  Handles wraparound for cases where the start value is
        /// bigger than the end value.  Used extensively as a convenience function to determine whether or not a
        /// piece of data belongs in a given location.
        ///
        /// Most typically, IsIDInRange is used to determine whether a key is between the local ID and the successor ID:
        ///     IsIDInRange(key, this.ID, this.Successor.ID);
        /// </summary>
        /// <param name="id">The ID to range-check.</param>
        /// <param name="start">The "low" end of the range.</param>
        /// <param name="end">The "high" end of the range.</param>
        /// <returns>TRUE if ID is in range; FALSE otherwise.</returns>
        public static bool IsIdInRange(BigInteger id, BigInteger start, BigInteger end)
        {
            if (start >= end)
            {
                // this handles the wraparound and single-node case.  for wraparound, the range includes zero, so any key
                // that is bigger than start or smaller than or equal to end is in the range.  for single-node, our nodehash
                // will equal the successor nodehash (we are our own successor), and there's no way a key can't fall in the range
                // because if range == X, then key must be either >, < or == X which will always happen!
                if (id > start || id <= end)
                {
                    return true;
                }
            }
            else
            {
                // this is the normal case where we want the key to fall between the lower bound of start and the upper bound of end
                if (id > start && id <= end)
                {
                    return true;
                }
            }
            // for all other cases we're not in range
            return false;
        }

        /// <summary>
        /// Range checks to determine if key fits in the range.  In this particular case, if the start==end of the range,
        /// we consider key to be in that range.  Handles wraparound.
        /// </summary>
        /// <param name="key">the key to range check</param>
        /// <param name="start">lower bound of the range</param>
        /// <param name="end">upper bound of the range</param>
        /// <returns>true if in the range; false if key is not in the range</returns>
        public static bool FingerInRange(BigInteger key, BigInteger start, BigInteger end)
        {
            // in this case, we are the successor of the predecessor we're looking for
            // so we return true which will mean return the farthest finger from FindClosestPrecedingFinger
            // ... this way, we can go as far around the circle as we know how to in order to find the
            // predecessor
            if (start == end)
            {
                return true;
            }

            if (start > end)
            {
                // this handles the wraparound case - since the range includes zero, any key bigger than the start
                // or smaller than the end will be considered in the range
                if (key > start || key < end)
                {
                    return true;
                }
            }
            else
            {
                // this is the normal case - in this case, the start is the lower bound and the end is the upper bound
                // so if key falls between them, we're good
                if (key > start && key < end)
                {
                    return true;
                }
            }
            // for all other cases, we're not in the range
            return false;
        }
    }
}