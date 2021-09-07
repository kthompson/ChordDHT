using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using System.Threading.Tasks;
using Serilog;

namespace ChordDHT
{
    public readonly struct FingerTableEntry
    {
        public Node Successor { get; }
        public BigInteger StartValue { get; }

        public FingerTableEntry(Node successor, BigInteger startValue)
        {
            Successor = successor;
            StartValue = startValue;
        }
    }

    public class FingerTable : IEnumerable<FingerTableEntry>
    {
        private readonly ChordServer _server;

        /// <summary>
        /// The StartValues for each finger in the finger table.  Each StartValue is a sequential
        /// power of two (wrapped around the Chord ring) further from its previous finger.
        /// </summary>
        private readonly BigInteger[] _startValues;

        /// <summary>
        /// The parallel Successor array, where each Successor represents a cached version of FindSuccessor() on
        /// the corresponding StartValue from the StartValues array.
        /// </summary>
        private readonly Node[] _successors;

        /// <summary>
        /// the length of the fingerTable (equal to M or the number of bits in the hash key)
        /// </summary>
        private int _length;

        private int _nextFingerToUpdate;

        public FingerTable(Node seed, ChordServer server, int m = NodeId.Bits)
        {
            _server = server;
            _length = m;
            _startValues = new BigInteger[m];
            _successors = new Node[m];
            for (var i = 0; i < m; i++)
            {
                var value = seed.Id + BigInteger.Pow(2, i);
                _startValues[i] = value % NodeId.MaxValue;
                _successors[i] = seed;
            }
        }

        public async Task UpdateNextAsync()
        {
            // update the fingers moving outwards - once the last finger
            // has been reached, start again closest to LocalNode (0).
            if (_nextFingerToUpdate >= _length)
            {
                _nextFingerToUpdate = 0;
            }

            try
            {
                // Node validity is checked by findSuccessor
                var (_, successor) = await _server.FindSuccessorAsync(_startValues[_nextFingerToUpdate]);

                Log.Debug("Updating Successor for start value {StartValue} to ({Successor})", _startValues[_nextFingerToUpdate], successor);
                _successors[_nextFingerToUpdate] = successor;
            }
            catch (Exception exception)
            {
                Log.Error(exception, "Unable to update Successor for start value {StartValue} ({Message})",
                    _startValues[_nextFingerToUpdate], exception.Message);
            }

            _nextFingerToUpdate += 1;
        }


        public IEnumerator<FingerTableEntry> GetEnumerator()
        {
            for (int i = 0; i < _length; i++)
            {
                yield return new FingerTableEntry(_successors[i], _startValues[i]);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}