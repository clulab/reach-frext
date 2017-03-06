package org.clulab.friolo

import org.apache.logging.log4j.*
import groovy.json.*

/**
 * Class to transform and load REACH results files, in Fries Output JSON format, into a
 * format more suitable for searching entity and event interconnections via ElasticSearch.
 *   Written by: Tom Hicks. 9/10/2015.
 *   Last Modified: Rename event subtype field.
 */
class FrioFormer {

  static final Logger log = LogManager.getLogger(FrioFormer.class.getName());

  static final List INTERESTING_TYPES = ['activation', 'complex-assembly', 'regulation']

  // the provided instance of a class to load events into ElasticSearch
  public FrioLoader LOADER

  /** Public constructor taking a map of ingest option and an ElasticSearch loader class. */
  public FrioFormer (options, frioLoader) {
    log.trace("(FrioFormer.init): options=${options}, frioLoader=${frioLoader}")
    LOADER = frioLoader
  }


  /** Transform a single doc set from the given directory to a new JSON format
      and load it into an ElasticSearch engine. */
  def convert (directory, docId, tfMap) {
    log.trace("(FrioFormer.convert): dir=${directory}, docId=${docId}, tfMap=${tfMap}")

    // read JSON files into data structures:
    def jsonSlurper = new JsonSlurper(type: JsonParserType.INDEX_OVERLAY)
    def tjMap = tfMap.collectEntries { docType, filename ->
      def fRdr = new FileReader(new File(directory, filename))
      def json = jsonSlurper.parse(fRdr)
      if (json)                             // if JSON was parsed from file
        [(docType): json]
      else [:]                              // else JSON was missing in file
    }

    // extract relevant information from the data structures:
    tjMap['sentences'] = extractSentenceTexts(tjMap)  // must be extracted before others
    tjMap['entities']  = extractEntityMentions(tjMap) // must be extracted before events
    tjMap['events']    = extractEventMentions(tjMap)

    // convert one JSON format to another and load the events:
    def cnt = 0
    convertJson(docId, tjMap).each { event ->
      def jsonDocStr = JsonOutput.toJson(event)
      if (jsonDocStr) {
        def status = LOADER.addToIndex(jsonDocStr)
        if (status)
          cnt += 1
      }
    }
    return cnt                              // return number of events added
  }


  /** Create and return new JSON from the given type-to-json map for the specified document. */
  def convertJson (docId, tjMap) {
    log.trace("(FrioFormer.convertJson): docId=${docId}, tjMap=${tjMap}")
    def convertedEvents = []
    tjMap.events.each { id, event ->
      def evType = event.type ?: 'UNKNOWN'
      if (evType in INTERESTING_TYPES) {
        def newEvents = convertEvent(docId, tjMap, event)  // can be 1->N
        newEvents.each { convertedEvents << it }
      }
    }
    return convertedEvents
  }


  /** Return a shallow, tuple-like map created by processing the given event
   *  using the given document ID, map of document sentence texts, and map of document
   *  child texts (from controlled event mentions).
   */
  def convertEvent (docId, tjMap, event) {
    log.trace("(FrioFormer.convertEvent): docId=${docId}, event=${event}")

    def newEvents = []                      // list of new events created here

    // properties for the predicate portion of the output format
    def evType = event.type
    def predMap = ['type': evType]
    if (event?.subtype) predMap['subtype'] = event.subtype
    if (event?.regtype) predMap['regtype'] = event.regtype
    if (event?.sign) predMap['sign'] = event.sign
    if (event?.rule) predMap <<  ['rule': event.rule]

    // properties for the location portion of the output format
    def locMap = [:]
    if (event?.sentence) locMap <<  ['sentence': event.sentence]

    // handle activation or regulation
    if ((evType == 'activation') || (evType == 'regulation')) {
      def patient = getControlled(tjMap, event)
      if (patient && patient?.subtype) {    // kludge: retrieve the controlled events subtype
        predMap['subtype'] = patient.subtype // transfer it to the predicate where it belongs
        patient.remove('subtype')          // delete it from the patient where it was stashed
      }
      def agents = getControllers(tjMap, event)
      agents.each { agent ->
        def evMap = ['docId': docId, 'p': predMap, 'a': agent, 'loc': locMap]
        if (patient) evMap['t'] = patient
        newEvents << evMap
      }
    }

    // handle complex-assembly (aka binding)
    else if (evType == 'complex-assembly') {
      def themes = getThemes(tjMap, event)
      if (themes.size() == 2) {
        newEvents << ['docId': docId, 'p': predMap, 'loc': locMap,
                      'a': themes[0], 't': themes[1]]
        newEvents << ['docId': docId, 'p': predMap, 'loc': locMap,
                      'a': themes[1], 't': themes[0]]
      }
    }

    return newEvents
  }


  Map extractEntityMentions (tjMap) {
    log.trace("(FrioFormer.extractEntityMentions):")
    return tjMap.entities.frames.collectEntries { frame ->
      if (frame['frame-type'] == 'entity-mention') {
        def frameId = frame['frame-id']
        def frameMap = [ 'eText': frame['text'], 'eType': frame['type'] ]
        def frameXrefs = frame['xrefs']
        if (frameXrefs) {
          def namespaceInfo = extractNamespaceInfo(frame.xrefs)
          if (namespaceInfo)
            frameMap << namespaceInfo
        }
        [ (frameId): frameMap ]
      }
      else [:]                              // else frame was not an entity mention
    }
  }

  def extractNamespaceInfo (xrefList) {
    return xrefList.findResults { xref ->
      if (xref)                             // ignore (bad) empty xrefs (?)
        [ 'eNs': xref['namespace'], 'eId': xref['id'] ]
    }
  }


  Map extractEventMentions (tjMap) {
    log.trace("(FrioFormer.extractEventMentions):")
    return tjMap.events.frames.collectEntries { frame ->
      def frameId = frame['frame-id']
      def frameMap = [ 'id': frameId, 'type': frame['type'] ]
      if (frame?.subtype) frameMap['subtype'] = frame.subtype
      if ((frame.type == 'activation') || (frame.type == 'regulation'))
        frameMap['sign'] = extractSign(frame)
      if (frame['found-by']) frameMap['rule'] = frame['found-by']
      def sentence = lookupSentence(tjMap, frame?.sentence)
      if (sentence) frameMap['sentence'] = sentence
      frameMap['args'] = extractArgInfo(frame.arguments)
      [ (frameId): frameMap ]
    }
  }

  def extractArgInfo (argList) {
    return argList.collect { arg ->
      def aMap = [ 'role': arg['type'],
                   'text': arg['text'],
                   'argType': arg['argument-type'] ]
      if (arg.get('arg'))
        aMap << ['xref': arg['arg']]
      if (arg.get('args'))
        aMap['xrefs'] = arg.get('args')
      return aMap
    }
  }

  def extractSign (frame) {
    log.trace("(extractSign): frame=${frame?.subtype}")
    if (frame && frame?.subtype) {
      if (frame.subtype.startsWith('pos'))
        return 'positive'
      else if (frame.subtype.startsWith('neg'))
        return 'negative'
    }
    return 'UNKNOWN'                        // signal failure: should never happen
  }

  /** Return a map of frameId to sentence text for all sentences in the given doc map. */
  Map extractSentenceTexts (tjMap) {
    log.trace("(FrioFormer.extractSentenceTexts):")
    return tjMap.sentences.frames.collectEntries { frame ->
      if (frame['frame-type'] == 'sentence')
        [ (frame['frame-id']): frame['text'] ]
      else [:]
    }
  }


  /** Return a single map of salient properties from the named arguments of the given event. */
  def getArgByRole (event, role) {
    def argsWithRoles = getArgsByRole(event, role)
    return (argsWithRoles.size() > 0) ? argsWithRoles[0] : []
  }

  /** Return a list of maps of salient properties from the named arguments of the given event. */
  def getArgsByRole (event, role) {
    return event.args.findResults { if (it?.role == role) it }
  }

  /** Return a list of entity maps from the arguments in the given event arguments list. */
  def derefEntities (tjMap, argsList) {
    argsList.findResults { arg ->
      lookupEntity(tjMap, getEntityXref(arg))
    }
  }

  /** Return an entity map from the given entity argument map. */
  def derefEntity (tjMap, arg) {
    if (!arg) return null                   // sanity check
    lookupEntity(tjMap, getEntityXref(arg))
  }

  /** Return an event map from the given event argument map. */
  def derefEvent (tjMap, arg) {
    if (!arg) return null                   // sanity check
    lookupEvent(tjMap, getEventXref(arg))
  }

  /** Check that the given argument map refers to an entity and return the
      entity cross-reference it contains, or else null. */
  def getEntityXref (arg) {
    if (!arg) return null                   // sanity check
    return ((arg?.argType == 'entity') && arg?.xref) ? arg.xref : null
  }

  /** Check that the given argument map refers to an event and return the
      event cross-reference it contains, or else null. */
  def getEventXref (arg) {
    if (!arg) return null                   // sanity check
    return ((arg?.argType == 'event') && arg?.xref) ? arg.xref : null
  }

  /** Return a controlled entity map from the controlled argument of the given event. */
  def getControlled (tjMap, event) {
    log.trace("(getControlled): event=${event}")
    def ctrld = getArgByRole(event, 'controlled') // should be just 1 controller arg
    if (!ctrld) return null                       // nothing controlled: exit out now
    if (ctrld.argType == 'event') {               // if it has a controlled (nested) event
      def ctrldEvent = derefEvent(tjMap, ctrld)   // get the nested event
      if (!ctrldEvent) return null                // bad nesting: exit out now
      def ctrldEntity = getThemes(tjMap, ctrldEvent)?.getAt(0) // should be just 1 theme arg
      if (!ctrldEntity) return null               // bad nesting: exit out now
      if (ctrldEvent?.subtype)                    // stash the controlled event subtype
        ctrldEntity['subtype'] = ctrldEvent.subtype // and return it in the entity
      return ctrldEntity                    // return the nested entity
    }
    else                                    // else it is a directly controlled entity
      return derefEntity(tjMap, ctrld)
  }

  /** Return a list of controller entity maps from the controller argument of the given event. */
  def getControllers (tjMap, event) {
    log.trace("(getControllers): event=${event}")
    def ctlr = getArgByRole(event, 'controller') // should be just 1 controller arg
    if (ctlr)  {
      if (ctlr.argType == 'complex') {
        def themeRefs = getXrefsByPrefix(ctlr?.xrefs, 'theme')
        return themeRefs.findResults { xref -> lookupEntity(tjMap, xref) }
      }
      else                                  // else it is a single controller event
        return derefEntities(tjMap, [ctlr])
    }
  }

  /** Return a list of theme entity maps from the theme arguments of the given event. */
  def getThemes (tjMap, event) {
    log.trace("(getThemes): event=${event}")
    def themeArgs = getArgsByRole(event, 'theme')
    return derefEntities(tjMap, themeArgs)
  }


  /** Return a list of mention cross references from the arguments in the given
      cross reference map whose keys begins with the given prefix string. */
  def getXrefsByPrefix (xrefsMap, prefix) {
    if (!xrefsMap) return null              // sanity check
    return xrefsMap.findResults { xrRole, xref ->
      if (xrRole.startsWith(prefix))  return xref
    }
  }

  /** Return the entity map referenced by the given entity cross-reference or null. */
  def lookupEntity (tjMap, xref) {
    if (!xref) return null                  // sanity check
    return tjMap['entities'].get(xref)
  }

  /** Return the event map referenced by the given event cross-reference or null. */
  def lookupEvent (tjMap, xref) {
    if (!xref) return null                  // sanity check
    return tjMap['events'].get(xref)
  }

  /** Return a map of salient properties from the given entity cross-reference. */
  def lookupSentence (tjMap, sentXref) {
    if (!sentXref) return null              // propogate null
    return tjMap['sentences'].get(sentXref)
  }

}
