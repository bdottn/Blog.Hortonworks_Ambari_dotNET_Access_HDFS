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
    }
}