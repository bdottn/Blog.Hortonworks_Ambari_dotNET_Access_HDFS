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

        public bool CreateDirectory(string path)
        {
            // 傳入路徑不包含根目錄時，預設會在根目錄「/」底下
            return this.webHDFSClient.CreateDirectory(path).Result;
        }

        public bool DeleteDirectory(string path)
        {
            // 傳入路徑不包含根目錄時，預設會在根目錄「/」底下
            return this.webHDFSClient.DeleteDirectory(path).Result;
        }

        public string CreateFile(string localFile, string remotePath)
        {
            // 傳入遠端路徑不包含根目錄時，預設會在根目錄「/」底下
            return this.webHDFSClient.CreateFile(localFile, remotePath).Result;
        }

        public bool DeleteFile(string path)
        {
            // 傳入路徑不包含根目錄時，預設會在根目錄「/」底下
            return this.webHDFSClient.DeleteDirectory(path).Result;
        }
    }
}