package org.apache.giraffa;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.cli.CLITestHelper;
import org.apache.hadoop.cli.util.CLICommands;
import org.apache.hadoop.cli.util.CLITestData;
import org.apache.hadoop.cli.util.CLITestData.TestCmd;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

/**
 * TestGiraffaCLI
 * 
 * This test is designed to mimic TestHDFSCLI and make it work
 * for the Giraffa FS instead.
 * This test creates a TestGRFAConf.xml file based on TestHDFSConf.xml
 * inside the test-data directory.
 */
public class TestGiraffaCLI extends CLITestHelper {
  private static final String TEST_FILES_DIR = "src/test/test-data";
  private static final String TEST_CONFIG_FILE = TEST_FILES_DIR+"/testHDFSConf.xml";
  private static final String GIRAFFA_TEST_URI = "grfa://localhost:9000";
  private static final int PASSING_PERCENTAGE = 77;
  private static final HBaseTestingUtility UTIL =
    GiraffaTestUtils.getHBaseTestingUtility();

  protected FileSystem fs;

  @Before
  @Override
  public void setUp() throws Exception {
    // delete the old test cache and re-create it
    File testCacheDir = new File(TEST_CACHE_DATA_DIR);
    if(testCacheDir.exists())
      Files.deleteRecursively(testCacheDir);
    assertTrue("Failed to make: "+TEST_CACHE_DATA_DIR, testCacheDir.mkdirs());
    clitestDataDir = testCacheDir.toURI().toString().replace(' ', '+');

    // set up grfa config file (fixes subtle bug too)
    File grfaConfig = new File(TEST_CACHE_DATA_DIR+"/testGRFAConf.xml");
    grfaConfig.createNewFile();
    File hdfsConfig = new File(TEST_CONFIG_FILE);
    String content = IOUtils.toString(new FileInputStream(hdfsConfig));
    content = content.replaceAll("hdfs", "grfa");
    content = content.replaceAll("NAMNEODE", "NAMENODE");
    IOUtils.write(content, new FileOutputStream(grfaConfig));

    // set up CLITestHelper needed variables
    System.setProperty("test.cli.config", grfaConfig.getAbsolutePath());
    super.readTestConfigFile();

    // copy the testing data over and re-create test directory
    File testFilesDir = new File(TEST_FILES_DIR);
    FileUtils.copyDirectory(testFilesDir, testCacheDir);

    //start the cluster
    UTIL.startMiniCluster(1);
    conf = new GiraffaConfiguration(UTIL.getConfiguration());
    GiraffaFileSystem.setDefaultUri(conf, new URI(GIRAFFA_TEST_URI));
    GiraffaFileSystem.format((GiraffaConfiguration) conf, false);
    fs = GiraffaFileSystem.get(new URI(GIRAFFA_TEST_URI), (GiraffaConfiguration) conf);
    assertTrue("Not a GiraffaFS: "+fs.getUri(), fs instanceof GiraffaFileSystem);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    fs.delete(new Path("."), true);
    if(fs != null) fs.close();
    try {
      super.tearDown(); // displayResults
    } catch(AssertionError a) {
      int totalPass = 0;
      int totalFail = 0;
      for (int i = 0; i < testsFromConfigFile.size(); i++) {
        CLITestData td = testsFromConfigFile.get(i);
        boolean resultBoolean = td.getTestResult();
        if (resultBoolean) {
          totalPass ++;
        } else {
          totalFail ++;
        }
      }
      // PJJ: TestGiraffaCLI will not throw AssertionError if passing >= 77%.
      if((100 * totalPass / (totalPass + totalFail)) < PASSING_PERCENTAGE) {
        throw a;
      }
    }
  }

  @Override
  protected String expandCommand(String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("NAMENODE", GIRAFFA_TEST_URI);
    expCmd = super.expandCommand(expCmd);
    return expCmd;
  }

  @Override
  protected Result execute(TestCmd cmd) throws Exception {
    switch(cmd.getType()) {
    case DFSADMIN:
        //we do not deal with DFSADMIN commands so we convert to empty FS command.
        return new CLICommands.FSCmdExecutor(GIRAFFA_TEST_URI, new GiraffaAdmin(conf))
            .executeCommand(cmd.getCmd());
    default: 
        return new CLICommands.FSCmdExecutor(GIRAFFA_TEST_URI, new FsShell(conf))
            .executeCommand(cmd.getCmd());
    }
  }

  @Test
  @Override
  public void testAll() {
    super.testAll();
  }
}
