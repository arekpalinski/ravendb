using System;
using System.Diagnostics;
using Sparrow.Json;

namespace Sparrow
{
    public unsafe class UnmanagedMemory
    {
        private Memory<byte>? _memory;

        public readonly byte* Address;

        public readonly int Size;

        public Memory<byte> Memory
        {
            get
            {
                if (_memory.HasValue == false)
                {
                    var memoryManager = new UnmanagedMemoryManager(Address, Size);
                    _memory = memoryManager.Memory;
                }

                return _memory.Value;
            }
        }

        public UnmanagedMemory(byte* address, int size)
        {
            Address = address;
            Size = size;
        }

        private UnmanagedMemory(byte* address, Memory<byte> memory)
        {
            Address = address;
            Size = memory.Length;
            _memory = memory;
        }

        public UnmanagedMemory Slice(int start)
        {
            if (start == 0)
                return this;

            if (_memory.HasValue == false)
                return new UnmanagedMemory(Address + start, Size - start);

            var memory = _memory.Value.Slice(start);
            Debug.Assert(Size - start == memory.Length, "Size - start == memory.Length");

            return new UnmanagedMemory(Address + start, memory);
        }
    }
}
