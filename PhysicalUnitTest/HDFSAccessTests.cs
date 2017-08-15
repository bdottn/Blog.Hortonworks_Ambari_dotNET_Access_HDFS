using ExpectedObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

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
    }
}