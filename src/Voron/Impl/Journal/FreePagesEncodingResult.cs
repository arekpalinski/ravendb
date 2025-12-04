using System;
using Voron.Util.PFor;

namespace Voron.Impl.Journal;

public unsafe class FreePagesEncodingResult
{
    public int NumberOfEncodedPages { get; set; }

    public int EncodingSize { get; set; }
            
    public int NumberOfSections { get; set; }
            
    public int OverheadInPages { get; set; }
            
    public int SizeAndHeaders { get; set; }

    public int Write(byte* dst, FastPForEncoder encoder)
    {
        var totalCount = 0;
        var totalWrite = 0;

        var headerPtr = dst;
                
        var header = (FreePagesHeader*)headerPtr;
        header->NumberOfPages = NumberOfEncodedPages;
        header->EncodedSectionsCount = NumberOfSections;
                
        FreePagesSectionHeader* sectionHeaderPtr = (FreePagesSectionHeader*)(headerPtr + sizeof(FreePagesHeader));
                
        totalWrite += sizeof(FreePagesHeader) + sizeof(FreePagesSectionHeader) * NumberOfSections;

        var outputSize = EncodingSize;
                
        for (int i = 0; i < NumberOfSections; i++)
        {
            var (count, sizeUsed) = encoder.Write(dst + totalWrite, Math.Min(outputSize, FastPForEncoder.MaxOutputBufferSize));

            totalCount += count;
            totalWrite += sizeUsed;

            sectionHeaderPtr[i].Size = sizeUsed;
            outputSize -= sizeUsed;
        }
                
        if (totalCount != NumberOfEncodedPages)
            throw new InvalidOperationException($"Expected to encode and write {NumberOfEncodedPages} freed pages, but wrote {totalCount}");

        return totalWrite;
    }
}
