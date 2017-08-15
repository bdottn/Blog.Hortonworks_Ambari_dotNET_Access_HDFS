using ExpectedObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.IO;

namespace Physical.UnitTest
{
    [TestClass]
    public sealed class HDFSAccessTests
    {
        private HDFSAccess access;

        [TestInitialize]
        public void TestInitialize()
        {
            // HDFS cluster 客戶端進入端點設定於 AMBARI.LAB 主機上
            // 預設端點：http://[Ambari 主機名稱]:50070
            // 預設帳號：hdfs
            this.access = new HDFSAccess(@"http://AMBARI.LAB:50070", "hdfs");
        }

        [TestCleanup]
        public void TestCleanup()
        {
            // 取得根目錄資料夾
            var directories = this.access.GetDirectories(@"/");

            // 移除預設目錄：tmp、user 外的目錄
            foreach (var directory in directories)
            {
                if ("tmp".Equals(directory) || "user".Equals(directory))
                {
                    continue;
                }
                else
                {
                    this.access.DeleteDirectory(directory);
                }
            }

            // 取得根目錄檔案
            var files = this.access.GetFiles(@"/");

            // 移除所有檔案
            foreach (var file in files)
            {
                this.access.DeleteFile(file);
            }

            // 移除 OpenFile 轉存檔案
            File.Delete(Path.Combine(Directory.GetCurrentDirectory(), "Test.jpg"));
        }

        [TestMethod]
        public void GetDirectoriesTest_傳入根目錄_預期回傳預設目錄()
        {
            // 預設根目錄下有兩個目錄：tmp、user
            var expected = new List<string>() { "tmp", "user", };

            var actual = this.access.GetDirectories(@"/");

            expected.ToExpectedObject().ShouldEqual(actual);
        }

        [TestMethod]
        public void GetFilesTest_傳入根目錄_預期回傳空集合()
        {
            // 預設根目錄下沒有檔案
            var expected = new List<string>();

            var actual = this.access.GetFiles(@"/");

            expected.ToExpectedObject().ShouldEqual(actual);
        }

        [TestMethod]
        public void DirectoryTest_建立zzz目錄_預期成功_預期根目錄下有zzz目錄_刪除zzz目錄_預期成功_預期根目錄下無zzz目錄()
        {
            var directoryName = "zzz";

            // 建立zzz目錄
            var boolCreateDirectory = this.access.CreateDirectory(directoryName);

            // 建立zzz目錄_預期成功
            Assert.IsTrue(boolCreateDirectory);

            // 建立zzz目錄_預期成功_預期根目錄下有zzz目錄
            // 預設根目錄下有三個目錄：tmp、user、zzz
            var expectedCreateDirectory = new List<string>() { "tmp", "user", directoryName, };

            var actualCreateDirectory = this.access.GetDirectories(@"/");

            expectedCreateDirectory.ToExpectedObject().ShouldEqual(actualCreateDirectory);

            // 刪除zzz目錄
            var boolDeleteDirectory = this.access.DeleteDirectory(directoryName);

            // 刪除zzz目錄_預期成功
            Assert.IsTrue(boolCreateDirectory);

            // 刪除zzz目錄_預期成功_預期根目錄下無zzz目錄
            // 預設根目錄下有兩個目錄：tmp、user
            var expectedDeleteDirectory = new List<string>() { "tmp", "user", };

            var actualDeleteDirectory = this.access.GetDirectories(@"/");

            expectedDeleteDirectory.ToExpectedObject().ShouldEqual(actualDeleteDirectory);
        }

        [TestMethod]
        public void FileTest_建立Test檔案_預期根目錄下有Test檔案_刪除Test檔案_預期成功_預期根目錄下無Test檔案()
        {
            var localFile = Path.Combine(Directory.GetCurrentDirectory(), "TestFolder", "Test.jpg");
            var remotePath = "Test.jpg";

            // 建立Test檔案
            var boolCreateFile = this.access.CreateFile(localFile, remotePath);

            // 建立Test檔案_預期根目錄下有Test檔案
            var expectedCreateFile = new List<string>() { remotePath, };

            var actualCreateFile = this.access.GetFiles(@"/");

            expectedCreateFile.ToExpectedObject().ShouldEqual(actualCreateFile);

            // 刪除Test檔案
            var boolDeleteFile = this.access.DeleteDirectory(remotePath);

            // 刪除Test檔案_預期成功
            Assert.IsTrue(boolDeleteFile);

            // 刪除Test檔案_預期成功_預期根目錄下無Test檔案
            var expectedDeleteFile = new List<string>();

            var actualDeleteFile = this.access.GetFiles(@"/");

            expectedDeleteFile.ToExpectedObject().ShouldEqual(actualDeleteFile);
        }

        [TestMethod]
        public void OpenFileTest_建立Test檔案_預期根目錄下有Test檔案_取得Test檔案_預期成功_預期回傳Test檔案Stream並轉存成功()
        {
            var localFile = Path.Combine(Directory.GetCurrentDirectory(), "TestFolder", "Test.jpg");
            var remotePath = "Test.jpg";
            var saveFile = Path.Combine(Directory.GetCurrentDirectory(), "Test.jpg");

            Assert.IsFalse(File.Exists(saveFile));

            // 建立Test檔案
            var boolCreateFile = this.access.CreateFile(localFile, remotePath);

            // 建立Test檔案_預期根目錄下有Test檔案
            var expectedCreateFile = new List<string>() { remotePath, };

            var actualCreateFile = this.access.GetFiles(@"/");

            expectedCreateFile.ToExpectedObject().ShouldEqual(actualCreateFile);

            // 取得Test檔案
            var response = this.access.OpenFile(remotePath);

            // 取得Test檔案_預期成功
            response.EnsureSuccessStatusCode();

            // 取得Test檔案_預期成功_預期回傳Test檔案Stream並轉存成功
            using (var fs = File.Create(saveFile))
            {
                response.Content.CopyToAsync(fs).Wait();
            }

            Assert.IsTrue(File.Exists(saveFile));
        }
    }
}