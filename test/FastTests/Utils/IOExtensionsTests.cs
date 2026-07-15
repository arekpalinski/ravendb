using System;
using System.IO;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Utils
{
    public class IOExtensionsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public void IsCrossDeviceMove_OnPosix_ClassifiesByExDevErrno()
        {
            var exdev = new IOException("Invalid cross-device link", hresult: 18);
            Assert.True(IOExtensions.IsCrossDeviceMove(exdev, "/mnt/a/db", "/mnt/b/db", runningOnPosix: true));

            var eacces = new IOException("Permission denied", hresult: 13);
            Assert.False(IOExtensions.IsCrossDeviceMove(eacces, "/mnt/a/db", "/mnt/b/db", runningOnPosix: true));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void IsCrossDeviceMove_OnWindows_ClassifiesByNotSameDeviceHResult()
        {
            var notSameDevice = new IOException("The system cannot move the file to a different disk drive.", hresult: unchecked((int)0x80070011));
            Assert.True(IOExtensions.IsCrossDeviceMove(notSameDevice, @"C:\data\db", @"C:\mount\db", runningOnPosix: false));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void IsCrossDeviceMove_OnWindows_ClassifiesByPathRootMismatch()
        {
            // Directory.Move pre-checks path roots and throws with a generic HResult, so classification falls back to comparing roots
            var genericIoException = new IOException("Source and destination path must have identical roots. Move will not work across volumes.");
            Assert.True(IOExtensions.IsCrossDeviceMove(genericIoException, @"C:\data\db", @"D:\data\db", runningOnPosix: false));
            Assert.False(IOExtensions.IsCrossDeviceMove(genericIoException, @"C:\data\db", @"C:\data\db-old", runningOnPosix: false));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void CopyDirectoryAndDeleteSource_CopiesNestedTreeAndRemovesSource()
        {
            var root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try
            {
                var src = Path.Combine(root, "src");
                var dst = Path.Combine(root, "dst");
                Directory.CreateDirectory(Path.Combine(src, "sub", "nested"));
                Directory.CreateDirectory(Path.Combine(src, "empty"));
                File.WriteAllText(Path.Combine(src, "a.txt"), "a");
                File.WriteAllText(Path.Combine(src, "sub", "b.txt"), "b");
                File.WriteAllText(Path.Combine(src, "sub", "nested", "c.txt"), "c");

                IOExtensions.CopyDirectoryAndDeleteSource(src, dst);

                Assert.False(Directory.Exists(src));
                Assert.Equal("a", File.ReadAllText(Path.Combine(dst, "a.txt")));
                Assert.Equal("b", File.ReadAllText(Path.Combine(dst, "sub", "b.txt")));
                Assert.Equal("c", File.ReadAllText(Path.Combine(dst, "sub", "nested", "c.txt")));
                Assert.True(Directory.Exists(Path.Combine(dst, "empty")));
            }
            finally
            {
                IOExtensions.DeleteDirectory(root);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void CopyDirectoryAndDeleteSource_OnFailure_KeepsSourceIntact()
        {
            var root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try
            {
                var src = Path.Combine(root, "src");
                var dst = Path.Combine(root, "dst");
                Directory.CreateDirectory(src);
                File.WriteAllText(Path.Combine(src, "a.txt"), "a");
                File.WriteAllText(dst, "blocking file at the destination directory path");

                Assert.ThrowsAny<IOException>(() => IOExtensions.CopyDirectoryAndDeleteSource(src, dst));

                Assert.True(Directory.Exists(src));
                Assert.Equal("a", File.ReadAllText(Path.Combine(src, "a.txt")));
                Assert.False(Directory.Exists(dst));
            }
            finally
            {
                IOExtensions.DeleteDirectory(root);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void TryGetResolvedDirectoryLinkTarget_ReturnsNullForRegularDirectoryAndMissingPath()
        {
            var root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try
            {
                Directory.CreateDirectory(root);

                Assert.Null(IOExtensions.TryGetResolvedDirectoryLinkTarget(root));
                Assert.Null(IOExtensions.TryGetResolvedDirectoryLinkTarget(Path.Combine(root, "does-not-exist")));
            }
            finally
            {
                IOExtensions.DeleteDirectory(root);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Core, RavenPlatform.Linux)]
        public void TryGetResolvedDirectoryLinkTarget_ResolvesSymlinkChainToFinalTarget()
        {
            var root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try
            {
                var target = Path.Combine(root, "target");
                var intermediate = Path.Combine(root, "intermediate");
                var link = Path.Combine(root, "link");
                Directory.CreateDirectory(target);
                Directory.CreateSymbolicLink(intermediate, target);
                Directory.CreateSymbolicLink(link, intermediate);

                var resolved = IOExtensions.TryGetResolvedDirectoryLinkTarget(link);

                Assert.Equal(new DirectoryInfo(target).FullName, resolved);
            }
            finally
            {
                IOExtensions.DeleteDirectory(root);
            }
        }
    }
}
