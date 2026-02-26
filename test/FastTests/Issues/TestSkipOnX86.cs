using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class TestSkipOnX86 : RavenTestBase
    {
        public TestSkipOnX86(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Test_SkipTestIfRunningOnX86_SkipsOnX86()
        {
            // This test validates that SkipTestIfRunningOnX86 correctly throws SkipTestException on x86
            // On x64, this test will pass normally
            // On x86, this test should be skipped with the custom message
            
            if (RuntimeInformation.ProcessArchitecture == Architecture.X86)
            {
                // On x86, expect SkipTestException to be thrown
                var exception = Assert.Throws<SkipTestException>(() => 
                    SkipTestIfRunningOnX86("Custom skip reason for x86"));
                Assert.Contains("Custom skip reason for x86", exception.Message);
            }
            else
            {
                // On x64, the method should not throw
                SkipTestIfRunningOnX86("Custom skip reason for x86");
                Assert.True(true); // Test passes on x64
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Test_SkipTestIfRunningOnX86_UsesDefaultMessage()
        {
            // Test that the default message is used when no reason is provided
            if (RuntimeInformation.ProcessArchitecture == Architecture.X86)
            {
                var exception = Assert.Throws<SkipTestException>(() => 
                    SkipTestIfRunningOnX86());
                Assert.Contains("not supported on x86 (32-bit) architecture", exception.Message);
            }
            else
            {
                SkipTestIfRunningOnX86();
                Assert.True(true);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task Test_ActualTestCase_SkippedOnX86()
        {
            // Simulate an actual test case that should be skipped on x86
            SkipTestIfRunningOnX86("This feature requires 64-bit architecture.");
            
            // This code will only execute on x64
            using (var store = GetDocumentStore())
            {
                await Task.Delay(10); // Simulate some work
                Assert.NotNull(store);
            }
        }
    }
}
