using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Client.Exceptions;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Voron.Util.Settings;

namespace Raven.Server.Utils
{
    public static class IOExtensions
    {
        private const int Retries = 50;

        public static EventHandler<(string Path, TimeSpan Duration, int Attempt)> AfterGc;

        public static void DeleteFile(string file)
        {
            try
            {
                File.Delete(file);
            }
            catch (IOException)
            {

            }
            catch (UnauthorizedAccessException)
            {

            }
        }

        public static void RenameFile(string oldFile, string newFile)
        {
            File.Move(oldFile, newFile);

            if (PlatformDetails.RunningOnPosix)
            {
                Syscall.FsyncDirectoryFor(newFile);
            }
        }

        public static bool EnsureReadWritePermissionForDirectory(string directory)
        {
            string tmpFileName = null;
            string testString = "I can write here!";
            try
            {
                if (string.IsNullOrWhiteSpace(directory))
                {
                    return false;
                }

                if (Directory.Exists(directory) == false)
                {
                    Directory.CreateDirectory(directory);
                }

                tmpFileName = Path.Combine(directory, Path.GetRandomFileName());
                File.WriteAllText(tmpFileName, testString);
                var read = File.ReadAllText(tmpFileName); //I can read too!
                File.Delete(tmpFileName);
                return read == testString;
            }
            catch
            {
                //We need to try and delete the file here too since we can't modify the return value from a finally block.                
                try
                {
                    if (File.Exists(tmpFileName))
                    {
                        File.Delete(tmpFileName);
                    }
                }
                catch
                {
                }
                return false;
            }
        }

        public static void MoveDirectory(string src, string dst)
        {
            if (PlatformDetails.RunningOnWindows)
                (src, dst) = NormalizeLongPathPrefixOnWindows(src, dst);

            for (var i = 0; i < Retries; i++)
            {
                try
                {
                    DeleteDirectory(dst);

                    SetDirectoryAttributes(src, FileAttributes.Normal);
                    SetDirectoryAttributes(dst, FileAttributes.Normal);

                    Directory.Move(src, dst);
                    return;
                }
                catch (IOException e) when (Directory.Exists(src) && IsCrossDeviceMove(e, src, dst))
                {
                    CopyDirectoryAndDeleteSource(src, dst);
                    return;
                }
                catch (Exception)
                {
                    if (i == Retries - 1)
                        throw;

                    RunGc(src, i);
                }
            }
        }

        internal static bool IsCrossDeviceMove(IOException e, string src, string dst)
        {
            return IsCrossDeviceMove(e, src, dst, PlatformDetails.RunningOnPosix);
        }

        internal static bool IsCrossDeviceMove(IOException e, string src, string dst, bool runningOnPosix)
        {
            if (runningOnPosix)
                return e.HResult == (int)Errno.EXDEV;

            const int errorNotSameDevice = unchecked((int)0x80070011); // HRESULT_FROM_WIN32(ERROR_NOT_SAME_DEVICE), thrown when a rename crosses volumes through a junction / mount point

            if (e.HResult == errorNotSameDevice)
                return true;

            // Directory.Move throws a generic IOException (COR_E_IO) from its own path-root pre-check before any syscall,
            // so a root mismatch is the only signal for the different-drive-letters case
            return string.Equals(Path.GetPathRoot(src), Path.GetPathRoot(dst), StringComparison.OrdinalIgnoreCase) == false;
        }

        internal static void CopyDirectoryAndDeleteSource(string src, string dst)
        {
            if (PlatformDetails.RunningOnWindows)
                (src, dst) = NormalizeLongPathPrefixOnWindows(src, dst); // also called directly (not only via MoveDirectory), e.g. from tests

            try
            {
                DeleteDirectory(dst);
                CopyDirectory(src, dst);
            }
            catch
            {
                DeleteDirectory(dst); // do not leave a partially copied destination behind
                throw;
            }

            DeleteDirectory(src);
        }

        private static void CopyDirectory(string src, string dst)
        {
            Directory.CreateDirectory(dst);

            string lastCopiedFile = null;

            foreach (var file in Directory.GetFiles(src))
            {
                var dstFile = Path.Combine(dst, Path.GetFileName(file));
                File.Copy(file, dstFile);

                using (var fs = new FileStream(dstFile, FileMode.Open, FileAccess.ReadWrite))
                    fs.Flush(flushToDisk: true);

                lastCopiedFile = dstFile;
            }

            if (lastCopiedFile != null && PlatformDetails.RunningOnPosix)
                Syscall.FsyncDirectoryFor(lastCopiedFile);

            foreach (var directory in Directory.GetDirectories(src))
                CopyDirectory(directory, Path.Combine(dst, Path.GetFileName(directory)));
        }

        internal static string TryGetResolvedDirectoryLinkTarget(string path)
        {
            try
            {
                if (Directory.ResolveLinkTarget(path, returnFinalTarget: true) is not DirectoryInfo target)
                    return null;

                // a volume root (e.g. \\?\Volume{guid}\ from a mount point) cannot host the sibling directories we derive from this path
                if (target.Exists == false || target.Parent == null)
                    return null;

                return target.FullName;
            }
            catch
            {
                return null;
            }
        }

        public static void DeleteDirectory(string directory)
        {
            for (var i = 0; i < Retries; i++)
            {
                try
                {
                    if (Directory.Exists(directory) == false)
                        return;

                    SetDirectoryAttributes(directory, FileAttributes.Normal);

                    Directory.Delete(directory, true);
                    return;
                }
                catch (IOException e)
                {
                    try
                    {
                        foreach (var childDir in Directory.GetDirectories(directory, "*", SearchOption.AllDirectories))
                        {
                            SetDirectoryAttributes(childDir, FileAttributes.Normal);
                        }
                    }
                    catch (IOException)
                    {
                    }
                    catch (UnauthorizedAccessException)
                    {
                    }

                    TryHandlingError(directory, i, e);
                }
                catch (UnauthorizedAccessException e)
                {
                    TryHandlingError(directory, i, e);
                }
            }
        }

        public static void CreateDirectory(string directory)
        {
            for (var i = 0; i < Retries; i++)
            {
                try
                {
                    if (Directory.Exists(directory))
                        return;

                    Directory.CreateDirectory(directory);
                    return;
                }
                catch (Exception)
                {
                    if (i == Retries - 1)
                        throw;

                    RunGc(directory, i);
                }
            }
        }

        private static void TryHandlingError(string directory, int i, Exception e)
        {
            if (i == Retries - 1) // last try also failed
            {
                foreach (var file in Directory.GetFiles(directory, "*", SearchOption.AllDirectories))
                {
                    var path = Path.GetFullPath(file);
                    try
                    {
                        File.Delete(path);
                    }
                    catch (Exception ex)
                    {
                        var message = UnsuccessfulFileAccessException.GetMessage(path, FileAccess.Write, ex);
                        throw new IOException(message, ex);
                    }
                }
                throw new IOException("Could not delete " + Path.GetFullPath(directory), e);
            }

            RunGc(directory, i);
        }

        private static void SetDirectoryAttributes(string path, FileAttributes attributes)
        {
            if (Directory.Exists(path) == false)
                return;

            try
            {
                File.SetAttributes(path, attributes);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        private static void RunGc(string path, int attempt)
        {
            Stopwatch sw = null;

            if (AfterGc != null)
                sw = Stopwatch.StartNew();

            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (AfterGc != null)
                AfterGc(null, (path, sw.Elapsed, attempt));

            Thread.Sleep(100);
        }


        private static (string Src, string Dst) NormalizeLongPathPrefixOnWindows(string src, string dst)
        {
            Debug.Assert(PlatformDetails.RunningOnWindows);

            var srcHasPrefix = src.StartsWith(PathUtil.LongPathPrefixWindows, StringComparison.OrdinalIgnoreCase);
            var dstHasPrefix = dst.StartsWith(PathUtil.LongPathPrefixWindows, StringComparison.OrdinalIgnoreCase);

            if (srcHasPrefix == dstHasPrefix)
                return (src, dst);

            return srcHasPrefix
                ? (src, PathUtil.LongPathPrefixWindows + dst)
                : (PathUtil.LongPathPrefixWindows + src, dst);
        }
    }
}
