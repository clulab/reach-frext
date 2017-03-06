package org.clulab.friolo

import java.net.InetAddress;
import java.util.concurrent.TimeUnit

import org.apache.logging.log4j.*;

/**
 * Class to load REACH results documents in JSON format into an ElasticSearch engine.
 *   Written by: Tom Hicks. 3/5/2017.
 *   Last Modified: Initial port of infrastructure.
 */
class FrextLoader {

  static final Logger log = LogManager.getLogger(FrextLoader.class.getName());

  Map settings

  /** Public constructor taking a map of ingest option. */
  public FrextLoader (Map settings) {
    log.trace("(FrextLoader.init): settings=${settings}")
    this.settings = settings                // save incoming settings in global variable:
  }

  /** Read the JSON ES configuration file, on the classpath, and return its text content. */
  def readJsonConfigFile (filepath) {
    def inStream = this.getClass().getResourceAsStream(filepath);
    if (inStream)
      return inStream.getText()             // read mapping text
    return null                             // signal failure to read
  }

  /** Shutdown and terminate this insertion node. */
  void exit () {
    log.trace("(FrextLoader.exit):")
    client.close()
  }

}
