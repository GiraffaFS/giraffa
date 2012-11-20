/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraffa;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class provides some administrative access.
 */
@InterfaceAudience.Private
public class GiraffaAdmin extends FsShell {

  /**
   * An abstract class for the execution of a file system command
   */
  abstract private static class GiraffaAdminCommand extends Command {
    final GiraffaFileSystem fs;
    /** Constructor */
    public GiraffaAdminCommand(FileSystem fs) {
      super(fs.getConf());
      if (!(fs instanceof GiraffaFileSystem)) {
        throw new IllegalArgumentException("FileSystem " + fs.getUri() + 
            " is not a Giraffa file system");
      }
      this.fs = (GiraffaFileSystem) fs;
    }
  }
  
  /** A class that supports command clearQuota */
  private static class ClearQuotaCommand extends GiraffaAdminCommand {
    private static final String NAME = "clrQuota";
    private static final String USAGE = "-"+NAME+" <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
    "Clear the quota for each directory <dirName>.\n" +
    "\t\tFor each directory, attempt to clear the quota. An error will be reported if\n" +
    "\t\t1. the directory does not exist or is a file, or\n" +
    "\t\t2. user is not an administrator.\n" +
    "\t\tIt does not fault if the directory has no quota.";
    
    /** Constructor */
    ClearQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 1, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the clrQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      fs.setQuota(path, FSConstants.QUOTA_RESET, FSConstants.QUOTA_DONT_SET);
    }
  }
  
  /** A class that supports command setQuota */
  private static class SetQuotaCommand extends GiraffaAdminCommand {
    private static final String NAME = "setQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname>";
    private static final String DESCRIPTION = 
      "-setQuota <quota> <dirname>...<dirname>: " +
      "Set the quota <quota> for each directory <dirName>.\n" + 
      "\t\tThe directory quota is a long integer that puts a hard limit\n" +
      "\t\ton the number of names in the directory tree\n" +
      "\t\tFor each directory, attempt to set the quota. An error will be reported if\n" +
      "\t\t1. N is not a positive integer, or\n" +
      "\t\t2. user is not an administrator, or\n" +
      "\t\t3. the directory does not exist or is a file, or\n";

    private final long quota; // the quota to be set
    
    /** Constructor */
    SetQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.quota = Long.parseLong(parameters.remove(0));
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the setQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a count command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      fs.setQuota(path, quota, FSConstants.QUOTA_DONT_SET);
    }
  }
  
  /** A class that supports command clearSpaceQuota */
  private static class ClearSpaceQuotaCommand extends GiraffaAdminCommand {
    private static final String NAME = "clrSpaceQuota";
    private static final String USAGE = "-"+NAME+" <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
    "Clear the disk space quota for each directory <dirName>.\n" +
    "\t\tFor each directory, attempt to clear the quota. An error will be reported if\n" +
    "\t\t1. the directory does not exist or is a file, or\n" +
    "\t\t2. user is not an administrator.\n" +
    "\t\tIt does not fault if the directory has no quota.";
    
    /** Constructor */
    ClearSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 1, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the clrQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a clrQuota command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      fs.setQuota(path, FSConstants.QUOTA_DONT_SET, FSConstants.QUOTA_RESET);
    }
  }
  
  /** A class that supports command setQuota */
  private static class SetSpaceQuotaCommand extends GiraffaAdminCommand {
    private static final String NAME = "setSpaceQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
      "Set the disk space quota <quota> for each directory <dirName>.\n" + 
      "\t\tThe space quota is a long integer that puts a hard limit\n" +
      "\t\ton the total size of all the files under the directory tree.\n" +
      "\t\tThe extra space required for replication is also counted. E.g.\n" +
      "\t\ta 1GB file with replication of 3 consumes 3GB of the quota.\n\n" +
      "\t\tQuota can also be speciefied with a binary prefix for terabytes,\n" +
      "\t\tpetabytes etc (e.g. 50t is 50TB, 5m is 5MB, 3p is 3PB).\n" + 
      "\t\tFor each directory, attempt to set the quota. An error will be reported if\n" +
      "\t\t1. N is not a positive integer, or\n" +
      "\t\t2. user is not an administrator, or\n" +
      "\t\t3. the directory does not exist or is a file, or\n";

    private long quota; // the quota to be set
    
    /** Constructor */
    SetSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      String str = parameters.remove(0).trim();
      quota = StringUtils.TraditionalBinaryPrefix.string2long(str);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the setQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a count command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      fs.setQuota(path, FSConstants.QUOTA_DONT_SET, quota);
    }
  }
  
  /**
   * Construct a GiraffaAdmin object.
   */
  public GiraffaAdmin() {
    this(null);
  }

  /**
   * Construct a GiraffaAdmin object.
   */
  public GiraffaAdmin(Configuration conf) {
    super(conf);
  }

  private void printHelp(String cmd) {
    String summary = "hadoop dfsadmin is the command to execute DFS administrative commands.\n" +
      "The full syntax is: \n\n" +
      "hadoop dfsadmin [-report] [-safemode <enter | leave | get | wait>]\n" +
      "\t[-saveNamespace]\n" +
      "\t[-restoreFailedStorage true|false|check]\n" +
      "\t[-refreshNodes]\n" +
      "\t[" + SetQuotaCommand.USAGE + "]\n" +
      "\t[" + ClearQuotaCommand.USAGE +"]\n" +
      "\t[" + SetSpaceQuotaCommand.USAGE + "]\n" +
      "\t[" + ClearSpaceQuotaCommand.USAGE +"]\n" +
      "\t[-refreshServiceAcl]\n" +
      "\t[-refreshUserToGroupsMappings]\n" +
      "\t[refreshSuperUserGroupsConfiguration]\n" +
      "\t[-printTopology]\n" +
      "\t[-help [cmd]]\n";

    String report ="-report: \tReports basic filesystem information and statistics.\n";
        
    String safemode = "-safemode <enter|leave|get|wait>:  Safe mode maintenance command.\n" + 
      "\t\tSafe mode is a Namenode state in which it\n" +
      "\t\t\t1.  does not accept changes to the name space (read-only)\n" +
      "\t\t\t2.  does not replicate or delete blocks.\n" +
      "\t\tSafe mode is entered automatically at Namenode startup, and\n" +
      "\t\tleaves safe mode automatically when the configured minimum\n" +
      "\t\tpercentage of blocks satisfies the minimum replication\n" +
      "\t\tcondition.  Safe mode can also be entered manually, but then\n" +
      "\t\tit can only be turned off manually as well.\n";

    String saveNamespace = "-saveNamespace:\t" +
    "Save current namespace into storage directories and reset edits log.\n" +
    "\t\tRequires superuser permissions and safe mode.\n";

    String restoreFailedStorage = "-restoreFailedStorage:\t" +
    "Set/Unset/Check flag to attempt restore of failed storage replicas if they become available.\n" +
    "\t\tRequires superuser permissions.\n";
    
    String refreshNodes = "-refreshNodes: \tUpdates the set of hosts allowed " +
                          "to connect to namenode.\n\n" +
      "\t\tRe-reads the config file to update values defined by \n" +
      "\t\tdfs.hosts and dfs.host.exclude and reads the \n" +
      "\t\tentires (hostnames) in those files.\n\n" +
      "\t\tEach entry not defined in dfs.hosts but in \n" + 
      "\t\tdfs.hosts.exclude is decommissioned. Each entry defined \n" +
      "\t\tin dfs.hosts and also in dfs.host.exclude is stopped from \n" +
      "\t\tdecommissioning if it has aleady been marked for decommission.\n" + 
      "\t\tEntires not present in both the lists are decommissioned.\n";

    String finalizeUpgrade = "-finalizeUpgrade: Finalize upgrade of HDFS.\n" +
      "\t\tDatanodes delete their previous version working directories,\n" +
      "\t\tfollowed by Namenode doing the same.\n" + 
      "\t\tThis completes the upgrade process.\n";

    String upgradeProgress = "-upgradeProgress <status|details|force>: \n" +
      "\t\trequest current distributed upgrade status, \n" +
      "\t\ta detailed status or force the upgrade to proceed.\n";

    String metaSave = "-metasave <filename>: \tSave Namenode's primary data structures\n" +
      "\t\tto <filename> in the directory specified by hadoop.log.dir property.\n" +
      "\t\t<filename> will contain one line for each of the following\n" +
      "\t\t\t1. Datanodes heart beating with Namenode\n" +
      "\t\t\t2. Blocks waiting to be replicated\n" +
      "\t\t\t3. Blocks currrently being replicated\n" +
      "\t\t\t4. Blocks waiting to be deleted\n";

    String refreshServiceAcl = "-refreshServiceAcl: Reload the service-level authorization policy file\n" +
      "\t\tNamenode will reload the authorization policy file.\n";
    
    String refreshUserToGroupsMappings = 
      "-refreshUserToGroupsMappings: Refresh user-to-groups mappings\n";
    
    String refreshSuperUserGroupsConfiguration = 
      "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings\n";

    String printTopology = "-printTopology: Print a tree of the racks and their\n" +
                           "\t\tnodes as reported by the Namenode\n";
    
    String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n" +
      "\t\tis specified.\n";

    if ("report".equals(cmd)) {
      System.out.println(report);
    } else if ("safemode".equals(cmd)) {
      System.out.println(safemode);
    } else if ("saveNamespace".equals(cmd)) {
      System.out.println(saveNamespace);
    } else if ("restoreFailedStorage".equals(cmd)) {
      System.out.println(restoreFailedStorage);
    } else if ("refreshNodes".equals(cmd)) {
      System.out.println(refreshNodes);
    } else if ("finalizeUpgrade".equals(cmd)) {
      System.out.println(finalizeUpgrade);
    } else if ("upgradeProgress".equals(cmd)) {
      System.out.println(upgradeProgress);
    } else if ("metasave".equals(cmd)) {
      System.out.println(metaSave);
    } else if (SetQuotaCommand.matches("-"+cmd)) {
      System.out.println(SetQuotaCommand.DESCRIPTION);
    } else if (ClearQuotaCommand.matches("-"+cmd)) {
      System.out.println(ClearQuotaCommand.DESCRIPTION);
    } else if (SetSpaceQuotaCommand.matches("-"+cmd)) {
      System.out.println(SetSpaceQuotaCommand.DESCRIPTION);
    } else if (ClearSpaceQuotaCommand.matches("-"+cmd)) {
      System.out.println(ClearSpaceQuotaCommand.DESCRIPTION);
    } else if ("refreshServiceAcl".equals(cmd)) {
      System.out.println(refreshServiceAcl);
    } else if ("refreshUserToGroupsMappings".equals(cmd)) {
      System.out.println(refreshUserToGroupsMappings);
    } else if ("refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.out.println(refreshSuperUserGroupsConfiguration);
    } else if ("printTopology".equals(cmd)) {
      System.out.println(printTopology);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(report);
      System.out.println(safemode);
      System.out.println(saveNamespace);
      System.out.println(restoreFailedStorage);
      System.out.println(refreshNodes);
      System.out.println(finalizeUpgrade);
      System.out.println(upgradeProgress);
      System.out.println(metaSave);
      System.out.println(SetQuotaCommand.DESCRIPTION);
      System.out.println(ClearQuotaCommand.DESCRIPTION);
      System.out.println(SetSpaceQuotaCommand.DESCRIPTION);
      System.out.println(ClearSpaceQuotaCommand.DESCRIPTION);
      System.out.println(refreshServiceAcl);
      System.out.println(refreshUserToGroupsMappings);
      System.out.println(refreshSuperUserGroupsConfiguration);
      System.out.println(printTopology);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-report".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-report]");
    } else if ("-safemode".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-safemode enter | leave | get | wait]");
    } else if ("-saveNamespace".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-saveNamespace]");
    } else if ("-restoreFailedStorage".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-restoreFailedStorage true|false|check ]");
    } else if ("-refreshNodes".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshNodes]");
    } else if ("-finalizeUpgrade".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-finalizeUpgrade]");
    } else if ("-upgradeProgress".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-upgradeProgress status | details | force]");
    } else if ("-metasave".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
          + " [-metasave filename]");
    } else if (SetQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [" + SetQuotaCommand.USAGE+"]");
    } else if (ClearQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " ["+ClearQuotaCommand.USAGE+"]");
    } else if (SetSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [" + SetSpaceQuotaCommand.USAGE+"]");
    } else if (ClearSpaceQuotaCommand.matches(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " ["+ClearSpaceQuotaCommand.USAGE+"]");
    } else if ("-refreshServiceAcl".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshServiceAcl]");
    } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshUserToGroupsMappings]");
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-refreshSuperUserGroupsConfiguration]");
    } else if ("-printTopology".equals(cmd)) {
      System.err.println("Usage: java DFSAdmin"
                         + " [-printTopology]");
    } else {
      System.err.println("Usage: java DFSAdmin");
      System.err.println("           [-report]");
      System.err.println("           [-safemode enter | leave | get | wait]");
      System.err.println("           [-saveNamespace]");
      System.err.println("           [-restoreFailedStorage true|false|check]");
      System.err.println("           [-refreshNodes]");
      System.err.println("           [-finalizeUpgrade]");
      System.err.println("           [-upgradeProgress status | details | force]");
      System.err.println("           [-metasave filename]");
      System.err.println("           [-refreshServiceAcl]");
      System.err.println("           [-refreshUserToGroupsMappings]");
      System.err.println("           [-refreshSuperUserGroupsConfiguration]");
      System.err.println("           [-printTopology]");
      System.err.println("           ["+SetQuotaCommand.USAGE+"]");
      System.err.println("           ["+ClearQuotaCommand.USAGE+"]");
      System.err.println("           ["+SetSpaceQuotaCommand.USAGE+"]");
      System.err.println("           ["+ClearSpaceQuotaCommand.USAGE+"]");      
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * @param argv The parameters passed to this program.
   * @exception Exception if the filesystem does not exist.
   * @return 0 on success, non zero on error.
   */
  @Override
  public int run(String[] argv) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-safemode".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-report".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-saveNamespace".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-restoreFailedStorage".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshNodes".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-finalizeUpgrade".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-upgradeProgress".equals(cmd)) {
        if (argv.length != 2) {
          printUsage(cmd);
          return exitCode;
        }
    } else if ("-metasave".equals(cmd)) {
      if (argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshServiceAcl".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-refreshUserToGroupsMappings".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-printTopology".equals(cmd)) {
      if(argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    }
    
    // initialize GiraffaAdmin
    try {
      init();
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server"
                         + "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      System.err.println("Bad connection to DFS... command aborted.");
      return exitCode;
    }

    exitCode = 0;
    try {
      if (ClearQuotaCommand.matches(cmd)) {
        exitCode = new ClearQuotaCommand(argv, i, fs).runAll();
      } else if (SetQuotaCommand.matches(cmd)) {
        exitCode = new SetQuotaCommand(argv, i, fs).runAll();
      } else if (ClearSpaceQuotaCommand.matches(cmd)) {
        exitCode = new ClearSpaceQuotaCommand(argv, i, fs).runAll();
      } else if (SetSpaceQuotaCommand.matches(cmd)) {
        exitCode = new SetSpaceQuotaCommand(argv, i, fs).runAll();
      } else if ("-help".equals(cmd)) {
        if (i < argv.length) {
          printHelp(argv[i]);
        } else {
          printHelp("");
        }
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (Exception e) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": "
                         + e.getLocalizedMessage());
    } 
    return exitCode;
  }

  /**
   * main() has some simple utility methods.
   * @param argv Command line parameters.
   * @exception Exception if the filesystem does not exist.
   */
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new GiraffaAdmin(), argv);
    System.exit(res);
  }
}