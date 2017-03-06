package org.clulab.frext

import java.net.InetAddress;
import java.util.concurrent.TimeUnit

import org.apache.logging.log4j.*;

/**
 * Class to load REACH results documents, in JSON format, into an ES engine.
 *   Written by: Tom Hicks. 3/5/2017.
 *   Last Modified: Initial port of infrastructure.
 */
class FrextLoader {

  static final Logger log = LogManager.getLogger(FrextLoader.class.getName());

  Map settings

  /** Public constructor taking a map of settings. */
  public FrextLoader (Map settings) {
    log.trace("(FrextLoader.init): settings=${settings}")
    this.settings = settings                // save incoming settings in global variable:
  }

  /** Output the given document. */
  def outputDoc (String aDoc) {
    log.trace("(FrioLoader.outputDoc): aDoc=${aDoc}")
    // TODO: IMPLEMENT LATER
    return true
  }

  /** Read a JSON configuration file, on the classpath, and return its text content. */
  def readJsonConfigFile (filepath) {
    def inStream = this.getClass().getResourceAsStream(filepath);
    if (inStream)
      return inStream.getText()             // read mapping text
    return null                             // signal failure to read
  }


  /** Shutdown and terminate this node. */
  void exit () {
    log.trace("(FrextLoader.exit):")
    // nothing required at the moment
  }

}
