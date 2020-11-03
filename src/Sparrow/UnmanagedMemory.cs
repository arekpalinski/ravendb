using System;
using Sparrow.Json;

namespace Sparrow
{
    public readonly unsafe struct UnmanagedMemory
    {
        public readonly byte* Address;

        public readonly Memory<byte> Memory;

        public UnmanagedMemory(byte* address, Memory<byte> memory)
        {
            Address = address;
            Memory = memory;
        }

        public UnmanagedMemory(byte* address, int size)
        {
            Address = address;
            var memoryManager = new UnmanagedMemoryManager(address, size);
            Memory = memoryManager.Memory;
        }
    }
}
