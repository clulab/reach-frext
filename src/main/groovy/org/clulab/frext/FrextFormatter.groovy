package org.clulab.frext

import org.apache.logging.log4j.*
import groovy.json.*

/**
 * Class to transform REACH results files, in FRIES output JSON format, into a
 * format more suitable for loading into a Biopax program.
 *
 *   Written by: Tom Hicks. 3/5/2017.
 *   Last Modified: Begin rewriting i/o translation.
 */
class FrextFormatter {

  static final Logger log = LogManager.getLogger(FrextFormatter.class.getName());

  /** Public constructor taking a map of ingest option. */
  public FrextFormatter (options) {
    log.trace("(FrextFormatter.init): options=${options}")
  }


  /** Transform a single doc set from the given directory to a new JSON format. */
  def transform (directory, docId, tfMap) {
    log.trace("(FrextFormatter.transform): dir=${directory}, docId=${docId}, tfMap=${tfMap}")

    // read JSON files into data structures:
    def jsonSlurper = new JsonSlurper(type: JsonParserType.INDEX_OVERLAY)
    def friesMap = tfMap.collectEntries { docType, filename ->
      def fRdr = new FileReader(new File(directory, filename))
      def json = jsonSlurper.parse(fRdr)
      if (json)                             // if JSON was parsed from file
        [(docType): json]
      else [:]                              // else JSON was missing in file
    }

    // extract relevant information from the data structures:
    friesMap['sentences'] = extractSentenceTexts(friesMap)  // must be extracted before others
    friesMap['entities']  = extractEntityMentions(friesMap) // must be extracted before events
    friesMap['events']    = extractEventMentions(friesMap)

    // build a new document with the information extracted from the FRIES format
    def outDoc = getMetaData(docId, friesMap)
    outDoc['events'] = getEvents(docId, friesMap)
    outputDocument(docId, outDoc)
    return 1
  }

  /** Begin the output document for a single paper by adding any metadata. */
  def getMetaData (docId, friesMap) {
    return [ "docId": docId ]               // TODO: ADD MORE METADATA LATER?
  }

  /** Create and return a list of events information extracted from the given paper. */
  def getEvents (docId, friesMap) {
    log.trace("(FrextFormatter.getEvents): docId=${docId}")
    def outEvents = []
    friesMap.events.each { id, event ->
      def newEvents = transformEvent(docId, friesMap, event)  // can be 1->N
      newEvents.each { outEvents << it }
    }
    return outEvents
  }


  /** Return a shallow, tuple-like map created by processing the given event
   *  using the given document ID, map of document sentence texts, and map of document
   *  child texts (from controlled event mentions).
   */
  def transformEvent (docId, friesMap, event) {
    log.trace("(FrextFormatter.transformEvent): docId=${docId}, event=${event}")

    def newEvents = []                      // list of new events created here

    // properties for the predicate portion of the output format
    def evType = event.type
    def predMap = ['type': evType]
    if (event?.subtype) predMap['sub_type'] = event.subtype
    if (event?.regtype) predMap['regulation_type'] = event.regtype
    if (event?.sign) predMap['sign'] = event.sign
    // if (event?.rule) predMap <<  ['rule': event.rule]

    // handle activation or regulation
    if ((evType == 'activation') || (evType == 'regulation')) {
      def patient = getControlled(friesMap, event)
      if (patient && patient?.subtype) {      // kludge: retrieve the controlled events subtype
        predMap['sub_type'] = patient.subtype // transfer it to the predicate where it belongs
        patient.remove('subtype')             // delete it from the patient where it was stashed
      }
      def agents = getControllers(friesMap, event)
      agents.each { agent ->
        def evMap = [
         'participant_a': agent,
         'interaction_type': predMap,
         'sentence': event.sentence ?: ''
        ]
        if (patient) evMap['participant_b'] = patient
        newEvents << evMap
      }
    }

    // handle complex-assembly (aka binding)
    else if (evType == 'complex-assembly') {
      predMap['type'] = 'binds'             // rename 'complex-assembly'
      def themes = getThemes(friesMap, event)
      if (themes.size() == 2) {
        newEvents << [ 'participant_a': themes[0],
                       'participant_b': themes[1],
                       'interaction_type': predMap,
                       'sentence': event.sentence ?: '' ]
        newEvents << [ 'participant_a': themes[1],
                       'participant_b': themes[0],
                       'interaction_type': predMap,
                       'sentence': event.sentence ?: '' ]
      }
    }

    return newEvents
  }


  Map extractEntityMentions (friesMap) {
    log.trace("(FrextFormatter.extractEntityMentions):")
    return friesMap.entities.frames.collectEntries { frame ->
      if (frame['frame-type'] == 'entity-mention') {
        def frameId = frame['frame-id']
        def frameMap = [ 'entity_text': frame['text'], 'entity_type': frame['type'] ]
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
        // [ 'ns': xref['namespace'], 'id': xref['id'] ]
        [ 'identifier': (xref['namespace'] + ':' + xref['id']) ]
    }
  }


  Map extractEventMentions (friesMap) {
    log.trace("(FrextFormatter.extractEventMentions):")
    return friesMap.events.frames.collectEntries { frame ->
      def frameId = frame['frame-id']
      def frameMap = [ 'id': frameId, 'type': frame['type'] ]
      if (frame?.subtype) frameMap['subtype'] = frame.subtype
      if ((frame.type == 'activation') || (frame.type == 'regulation'))
        frameMap['sign'] = extractSign(frame)
      if (frame['found-by']) frameMap['rule'] = frame['found-by']
      def sentence = lookupSentence(friesMap, frame?.sentence)
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
  Map extractSentenceTexts (friesMap) {
    log.trace("(FrextFormatter.extractSentenceTexts):")
    return friesMap.sentences.frames.collectEntries { frame ->
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
  def derefEntities (friesMap, argsList) {
    argsList.findResults { arg ->
      lookupEntity(friesMap, getEntityXref(arg))
    }
  }

  /** Return an entity map from the given entity argument map. */
  def derefEntity (friesMap, arg) {
    if (!arg) return null                   // sanity check
    lookupEntity(friesMap, getEntityXref(arg))
  }

  /** Return an event map from the given event argument map. */
  def derefEvent (friesMap, arg) {
    if (!arg) return null                   // sanity check
    lookupEvent(friesMap, getEventXref(arg))
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
  def getControlled (friesMap, event) {
    log.trace("(getControlled): event=${event}")
    def ctrld = getArgByRole(event, 'controlled') // should be just 1 controller arg
    if (!ctrld) return null                       // nothing controlled: exit out now
    if (ctrld.argType == 'event') {               // if it has a controlled (nested) event
      def ctrldEvent = derefEvent(friesMap, ctrld)   // get the nested event
      if (!ctrldEvent) return null                // bad nesting: exit out now
      def ctrldEntity = getThemes(friesMap, ctrldEvent)?.getAt(0) // should be just 1 theme arg
      if (!ctrldEntity) return null               // bad nesting: exit out now
      if (ctrldEvent?.subtype)                    // stash the controlled event subtype
        ctrldEntity['subtype'] = ctrldEvent.subtype // and return it in the entity
      return ctrldEntity                    // return the nested entity
    }
    else                                    // else it is a directly controlled entity
      return derefEntity(friesMap, ctrld)
  }

  /** Return a list of controller entity maps from the controller argument of the given event. */
  def getControllers (friesMap, event) {
    log.trace("(getControllers): event=${event}")
    def ctlr = getArgByRole(event, 'controller') // should be just 1 controller arg
    if (ctlr)  {
      if (ctlr.argType == 'complex') {
        def themeRefs = getXrefsByPrefix(ctlr?.xrefs, 'theme')
        return themeRefs.findResults { xref -> lookupEntity(friesMap, xref) }
      }
      else                                  // else it is a single controller event
        return derefEntities(friesMap, [ctlr])
    }
  }

  /** Return a list of theme entity maps from the theme arguments of the given event. */
  def getThemes (friesMap, event) {
    log.trace("(getThemes): event=${event}")
    def themeArgs = getArgsByRole(event, 'theme')
    return derefEntities(friesMap, themeArgs)
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
  def lookupEntity (friesMap, xref) {
    if (!xref) return null                  // sanity check
    return friesMap['entities'].get(xref)
  }

  /** Return the event map referenced by the given event cross-reference or null. */
  def lookupEvent (friesMap, xref) {
    if (!xref) return null                  // sanity check
    return friesMap['events'].get(xref)
  }

  /** Return a map of salient properties from the given entity cross-reference. */
  def lookupSentence (friesMap, sentXref) {
    if (!sentXref) return null              // propogate null
    return friesMap['sentences'].get(sentXref)
  }

  /** Output the given document. */
  def outputDocument (String docId, Map document) {
    log.trace("(FrextFormatter.outputDocument): docId=${docId}")
    def jsonDoc = JsonOutput.prettyPrint(JsonOutput.toJson(document))
    // TODO: IMPLEMENT FILE OUTPUT LATER
    println(jsonDoc)                        // REMOVE LATER
    return true
  }

  //   convertJson(docId, friesMap).each { event ->
  //     def jsonEvent = JsonOutput.toJson(event)
  //     if (jsonEvent) {
  //       def status = outputEvent(docId, jsonEvent)
  //       if (status)
  //         cnt += 1
  //     }
  //   }
  //   return cnt                              // return number of events added

}
