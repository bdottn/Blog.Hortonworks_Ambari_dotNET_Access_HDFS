using Microsoft.Hadoop.WebHDFS;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Physical
{
    public sealed class HDFSAccess
    {
        private readonly WebHDFSClient webHDFSClient;

        public HDFSAccess(string uriString, string userName)
        {
            this.webHDFSClient = new WebHDFSClient(new Uri(uriString), userName);
        }

        public List<string> GetDirectories(string path)
        {
            var directoryStatus = this.webHDFSClient.GetDirectoryStatus(path).Result;

            return directoryStatus.Directories.Select(d => d.PathSuffix).ToList();
        }

        public List<string> GetFiles(string path)
        {
            var directoryStatus = this.webHDFSClient.GetDirectoryStatus(path).Result;

            return directoryStatus.Files.Select(d => d.PathSuffix).ToList();
        }
    }
}