#region Disclaimer / License
// Copyright (C) 2015, The Duplicati Team
// http://www.duplicati.com, info@duplicati.com
// 
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
// 
#endregion
using Duplicati.Library.Interface;
using Duplicati.Library.Utility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace Duplicati.CommandLine.BackendTool
{
    class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        public static int Main(string[] args)
        {
            Duplicati.Library.AutoUpdater.UpdaterManager.IgnoreWebrootFolder = true;
            return Duplicati.Library.AutoUpdater.UpdaterManager.RunFromMostRecent(typeof(Program).GetMethod("RealMain"), args);
        }

        public static int RealMain(string[] _args)
        {
            bool debugoutput = false;
            try
            {
                string[] firstBackendArgs = _args.Take(2).Concat(_args.Skip(2).TakeWhile(a => a.StartsWith("--", StringComparison.Ordinal))).ToArray();
                string[] secondBackendArgs = _args.Skip(2).SkipWhile(a => a.StartsWith("--", StringComparison.Ordinal)).ToArray();
                
                List<string> args = new List<string>(firstBackendArgs);
                Dictionary<string, string> options = Library.Utility.CommandLineParser.ExtractOptions(args);

                if (!options.ContainsKey("auth_password") && !string.IsNullOrEmpty(System.Environment.GetEnvironmentVariable("AUTH_PASSWORD")))
                    options["auth_password"] = System.Environment.GetEnvironmentVariable("AUTH_PASSWORD");

                if (!options.ContainsKey("auth_username") && !string.IsNullOrEmpty(System.Environment.GetEnvironmentVariable("AUTH_USERNAME")))
                    options["auth_username"] = System.Environment.GetEnvironmentVariable("AUTH_USERNAME");

                if (options.ContainsKey("tempdir") && !string.IsNullOrEmpty(options["tempdir"]))
                    Library.Utility.TempFolder.SystemTempPath = options["tempdir"];

                debugoutput = Duplicati.Library.Utility.Utility.ParseBoolOption(options, "debug-output");

                string command = null;
                if (args.Count >= 2)
                {
                    if (args[0].Equals("list", StringComparison.OrdinalIgnoreCase))
                        command = "list";
                    else if (args[0].Equals("get", StringComparison.OrdinalIgnoreCase))
                        command = "get";
                    else if (args[0].Equals("put", StringComparison.OrdinalIgnoreCase))
                        command = "put";
                    else if (args[0].Equals("delete", StringComparison.OrdinalIgnoreCase))
                        command = "delete";
                    else if (args[0].Equals("create-folder", StringComparison.OrdinalIgnoreCase))
                        command = "create";
                    else if (args[0].Equals("createfolder", StringComparison.OrdinalIgnoreCase))
                        command = "create";
                    else if (args[0].Equals("sync", StringComparison.OrdinalIgnoreCase))
                        command = "sync";
                }


                if (args.Count < 2 || String.Equals(args[0], "help", StringComparison.OrdinalIgnoreCase) || args[0] == "?" || command == null)
                {
                    if (command == null && args.Count >= 2)
                    {
                        Console.WriteLine("Unsupported command: {0}", args[0]);
                        Console.WriteLine();
                    }   
                    
                    Console.WriteLine("Usage: <command> <protocol>://<username>:<password>@<path> [filename]");
                    Console.WriteLine("Example: LIST ftp://user:pass@server/folder");
                    Console.WriteLine();
                    Console.WriteLine("Supported backends: " + string.Join(",", Duplicati.Library.DynamicLoader.BackendLoader.Keys));
                    Console.WriteLine("Supported commands: GET PUT LIST DELETE CREATEFOLDER");

                    return 200;
                }
                
                using(var backend = Library.DynamicLoader.BackendLoader.GetBackend(args[1], options))
                {
                    if (backend == null)
                        throw new UserInformationException("Backend not supported", "InvalidBackend");

                    if (command == "list")
                    {
                        if (args.Count != 2)
                            throw new UserInformationException(string.Format("too many arguments: {0}", string.Join(",", args)), "BackendToolTooManyArguments");
                        Console.WriteLine("{0}\t{1}\t{2}\t{3}", "Name", "Dir/File", "LastChange", "Size");

                        foreach (var e in backend.List())
                            Console.WriteLine("{0}\t{1}\t{2}\t{3}", e.Name, e.IsFolder ? "Dir" : "File", e.LastModification, e.Size < 0 ? "" : Library.Utility.Utility.FormatSizeString(e.Size));

                        return 0;
                    }
                    else if (command == "create")
                    {
                        if (args.Count != 2)
                            throw new UserInformationException(string.Format("too many arguments: {0}", string.Join(",", args)), "BackendToolTooManyArguments");

                        backend.CreateFolder();

                        return 0;
                    }
                    else if (command == "delete")
                    {
                        if (args.Count < 3)
                            throw new UserInformationException("DELETE requires a filename argument", "BackendToolDeleteRequiresAnArgument");
                        if (args.Count > 3)
                            throw new Exception(string.Format("too many arguments: {0}", string.Join(",", args)));
                        backend.Delete(Path.GetFileName(args[2]));

                        return 0;
                    }
                    else if (command == "get")
                    {
                        if (args.Count < 3)
                            throw new UserInformationException("GET requires a filename argument", "BackendToolGetRequiresAnArgument");
                        if (args.Count > 3)
                            throw new UserInformationException(string.Format("too many arguments: {0}", string.Join(",", args)), "BackendToolTooManyArguments");
                        if (File.Exists(args[2]))
                            throw new UserInformationException("File already exists, not overwriting!", "BackendToolFileAlreadyExists");
                        backend.Get(Path.GetFileName(args[2]), args[2]);

                        return 0;
                    }
                    else if (command == "put")
                    {
                        if (args.Count < 3)
                            throw new UserInformationException("PUT requires a filename argument", "BackendToolPutRequiresAndArgument");
                        if (args.Count > 3)
                            throw new UserInformationException(string.Format("too many arguments: {0}", string.Join(",", args)), "BackendToolTooManyArguments");

                        backend.PutAsync(Path.GetFileName(args[2]), args[2], CancellationToken.None).Wait();

                        return 0;
                    }
                    else if (command == "sync")
                    {
                        if (secondBackendArgs.Length == 0)
                            throw new UserInformationException("SYNC requires parameters for a second backend", "BackendToolSyncRequiresAnArgument");

                        Dictionary<string, string> secondBackendOptions = Library.Utility.CommandLineParser.ExtractOptions(new List<string>(secondBackendArgs));

                        using (var secondBackend = Library.DynamicLoader.BackendLoader.GetBackend(secondBackendArgs[0], secondBackendOptions))
                        {
                            if (secondBackend == null)
                                throw new UserInformationException("Backend not supported", "InvalidBackend");

                            Console.WriteLine("Listing source files...");
                            var sourceList = backend.List().ToList();

                            Console.WriteLine("Ensuring folder exists on destination...");
                            try
                            {
                                secondBackend.Test();
                            }
                            catch (FolderMissingException)
                            {
                                Console.WriteLine("Creating folder on destination...");
                                secondBackend.CreateFolder();
                            }

                            Console.WriteLine("Listing destination files...");
                            var destList = secondBackend.List().ToDictionary(destFile => destFile.Name, destFile => destFile, StringComparer.Ordinal);

                            HashSet<string> sourceFileNames = new HashSet<string>(StringComparer.Ordinal);
                            List<IFileEntry> folders = new List<IFileEntry>();
                            List<IFileEntry> skipCopy = new List<IFileEntry>();
                            List<IFileEntry> copy = new List<IFileEntry>(sourceList.Count);
                            foreach (IFileEntry file in sourceList)
                            {
                                sourceFileNames.Add(file.Name);
                                if (file.IsFolder)
                                {
                                    folders.Add(file);
                                }
                                else if (destList.TryGetValue(file.Name, out IFileEntry dest) && file.Size == dest.Size)
                                {
                                    // TODO: Add option to force overwriting files with the same size
                                    skipCopy.Add(file);
                                }
                                else
                                {
                                    copy.Add(file);
                                }
                            }

                            List<IFileEntry> delete = new List<IFileEntry>(destList.Count);
                            foreach (var destFile in destList.Values)
                            {
                                if (!destFile.IsFolder && !sourceFileNames.Contains(destFile.Name))
                                {
                                    delete.Add(destFile);
                                }
                            }

                            if (folders.Count > 0)
                            {
                                Console.WriteLine("Ignoring {0} folders", folders.Count);
                            }

                            long copySize = copy.Sum(f => f.Size);
                            if (copy.Count > 0)
                            {
                                Console.WriteLine("Syncing  {0} files ({1})", copy.Count, Utility.FormatSizeString(copySize));
                            }

                            long skipSize = skipCopy.Sum(f => f.Size);
                            if (skipCopy.Count > 0)
                            {
                                Console.WriteLine("Skipping {0} files ({1})", skipCopy.Count, Utility.FormatSizeString(skipSize));
                            }

                            // TODO: Option to disable delete behavior entirely
                            long deleteSize = delete.Sum(f => f.Size);
                            if (delete.Count > 0)
                            {
                                Console.WriteLine("Deleting {0} files ({1}) from destination", delete.Count, Utility.FormatSizeString(deleteSize));
                            }

                            // TODO: Option to disable in-memory streaming via command line if desired?
                            Action<IFileEntry> syncFile;
                            if (backend is IStreamingBackend firstStreamingBackend && secondBackend is IStreamingBackend secondStreamingBackend)
                            {
                                syncFile = file =>
                                {
                                    using (MemoryStream stream = new MemoryStream())
                                    {
                                        firstStreamingBackend.Get(file.Name, stream);
                                        stream.Position = 0;
                                        secondStreamingBackend.PutAsync(file.Name, stream, CancellationToken.None).Await();
                                    }
                                };
                            }
                            else
                            {
                                syncFile = file =>
                                {
                                    using (TempFile tempFile = new TempFile())
                                    {
                                        backend.Get(file.Name, tempFile.Name);
                                        secondBackend.PutAsync(file.Name, tempFile.Name, CancellationToken.None).Await();
                                    }
                                };
                            }

                            long copied = 0;
                            for (int i = 0; i < copy.Count; i++)
                            {
                                var file = copy[i];

                                bool success = false;
                                for (int retryCount = 0; retryCount < 3 && !success; retryCount++)
                                {
                                    if (retryCount > 0)
                                    {
                                        Thread.Sleep((int)Math.Pow(100, retryCount));
                                    }

                                    try
                                    {
                                        Console.WriteLine("{0}/{1} Syncing {2} ({3})", i + 1, copy.Count, file.Name, Utility.FormatSizeString(file.Size));
                                        Console.WriteLine("[{0,-100}] {1:P}", new string('=', (int)(((double)copied / copySize) * 100)), (double)copied / copySize);
                                        syncFile(file);
                                        success = true;
                                    }
                                    catch (System.Net.WebException ex)
                                    {
                                        string response = null;
                                        using (Stream responseStream = ex.Response.GetResponseStream())
                                        using (var reader = new System.IO.StreamReader(responseStream))
                                        {
                                            response = reader.ReadToEnd();
                                        }

                                        Console.WriteLine("Caught WebException '{0}' and retrying:\n{1}\n{2}", ex.Message, ex, response);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine("Caught Exception '{0}' and retrying:\n{1}", ex.Message, ex);
                                    }
                                }

                                if (!success)
                                {
                                    throw new Exception("Sync failed - retry count exceeded");
                                }

                                copied += file.Size;
                            }

                            long deleted = 0;
                            for (int i = 0; i < delete.Count; i++)
                            {
                                var file = delete[i];

                                bool success = false;
                                for (int retryCount = 0; retryCount < 3 && !success; retryCount++)
                                {
                                    if (retryCount > 0)
                                    {
                                        Thread.Sleep((int)Math.Pow(100, retryCount));
                                    }

                                    try
                                    {
                                        Console.WriteLine("{0}/{1} Deleting {2} ({3})", i + 1, delete.Count, file.Name, Utility.FormatSizeString(file.Size));
                                        Console.WriteLine("[{0,-100}] {1:P}", new string('=', (int)(((double)deleted / deleteSize) * 100)), (double)deleted / deleteSize);
                                        secondBackend.Delete(file.Name);
                                        success = true;
                                    }
                                    catch (System.Net.WebException ex)
                                    {
                                        string response = null;
                                        using (Stream responseStream = ex.Response.GetResponseStream())
                                        using (var reader = new System.IO.StreamReader(responseStream))
                                        {
                                            response = reader.ReadToEnd();
                                        }

                                        Console.WriteLine("Caught WebException '{0}' and retrying:\n{1}\n{2}", ex.Message, ex, response);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine("Caught Exception '{0}' and retrying:\n{1}", ex.Message, ex);
                                    }
                                }

                                if (!success)
                                {
                                    throw new Exception("Sync failed - retry count exceeded");
                                }

                                deleted += file.Size;
                            }

                            Console.WriteLine("Sync complete");
                        }

                        return 0;
                    }

                    throw new Exception("Internal error");
                }
                
            }
            catch (Exception ex)
            {
                Console.WriteLine("Command failed: " + ex.Message);
                if (debugoutput || !(ex is UserInformationException))
                    Console.WriteLine(ex);
                return 100;
            }
        }
    }
}
