using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using Voron.Exceptions;

namespace Voron.Recovery.Journals
{
    public class JournalsRecovery
    {
        private readonly Func<StorageEnvironmentOptions> _recreateOptions;
        private readonly TextWriter _writer;

        public JournalsRecovery(Func<StorageEnvironmentOptions> recreateOptions, TextWriter writer)
        {
            _recreateOptions = recreateOptions;
            _writer = writer;
        }

        public StorageEnvironment RecoverByLoadingEnvironment(StorageEnvironmentOptions options)
        {
            _writer.WriteLine("Recovering journal files, this may take a while...");

            var sw = Stopwatch.StartNew();

            StorageEnvironment environment = null;

            bool optionOwnsPagers = options.OwnsPagers;
            try
            {
                options.OwnsPagers = false;

                while (true)
                {
                    try
                    {
                        environment = new StorageEnvironment(options);
                        break;
                    }
                    catch (IncreasingDataFileInCopyOnWriteModeException ex)
                    {
                        options.Dispose();

                        using (var file = File.Open(ex.DataFilePath, FileMode.Open))
                        {
                            file.SetLength(ex.RequestedSize);
                        }

                        options = _recreateOptions();
                    }
                    catch (OutOfMemoryException e)
                    {
                        if (e.InnerException is Win32Exception)
                            throw;
                    }
                }

                _writer.WriteLine(
                    $"Journal recovery has completed successfully within {sw.Elapsed.TotalSeconds:N1} seconds");
            }
            catch (Exception e)
            {
                environment?.Dispose();

                if (e is OutOfMemoryException && e.InnerException is Win32Exception)
                {
                    e.Data["ReturnCode"] = 0xDEAD;

                    _writer.WriteLine($"{e.InnerException.Message}. {e.Message}.");
                    _writer.WriteLine();
                    _writer.WriteLine("Journal recovery failed. To continue, please backup your files and run again with --DisableCopyOnWriteMode flag.");
                    _writer.WriteLine("Please note that this is usafe operation and we highly recommend to backup you files.");

                    throw;
                }

                _writer.WriteLine("Journal recovery failed, don't worry we will continue with data recovery.");
                _writer.WriteLine("The reason for the Jornal recovery failure was:");
                _writer.WriteLine(e);
            }
            finally
            {
                options.OwnsPagers = optionOwnsPagers;
            }

            return environment;
        }
    }
}
