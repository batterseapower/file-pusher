import java.io.{FileNotFoundException, IOException}
import java.lang.Thread.UncaughtExceptionHandler
import java.nio.file._
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.util.concurrent.TimeUnit
import java.time.Instant

import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.collection.mutable

case class Copy(fromMTime : Instant, from : Path, to : Path) extends Comparable[Copy] {
  def compareTo(that : Copy) = this.fromMTime.compareTo(that.fromMTime)
}

object Daemon extends App {
  val daemon = new Daemon(Paths.get("/Users/mbolingbroke/Junk/temp1"), Paths.get("/Users/mbolingbroke/Junk/temp2"))
}

class Daemon(fromRoot : Path, toRoot : Path) extends StrictLogging {
  val watchService = fromRoot.getFileSystem.newWatchService()
  // Ubuntu/Windows allegedly reports file changes almost instantly, but OS X at least seems to have a substantial delay
  // (around 5s?). For safety, say that we don't get notified until quite a while after the change has occurred.
  val WATCH_SERVICE_DELAY_SECONDS = 15

  val workQueue = new java.util.concurrent.PriorityBlockingQueue[Copy]()

  val isRegistered = new mutable.HashMap[Path, WatchKey]()
  def tryRegister(from : Path) = try {
    val watchKey = from.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.OVERFLOW)
    isRegistered.put(from, watchKey)
    logger.info(s"Registered $from")
    Some(watchKey)
  } catch {
    case (e : NotDirectoryException) => None
    case (e : IOException) => {
      logger.info(s"Failed to register $from", e)
      None
    }
  }

  def getMTime(from : Path) = try { Files.getLastModifiedTime(from).toInstant } catch { case (_ : IOException) => Instant.ofEpochMilli(0) }

  def registerTree(from : Path) : Instant = {
    val watchKey = tryRegister(from)
    val mTime    = getMTime(from)
    watchKey match {
      case None    => mTime // Either a file or a directory that just went AWOL
      case Some(_) => registerTreeChildren(from, mTime)
    }
  }

  def registerTreeChildren(from : Path, mTime : Instant) : Instant
    = (Files.list(from).iterator().asScala.map(registerTree).toSeq :+ mTime).max

  // NB: this might enqueue work items that duplicate existing ones in the queue, the copy thread just has to deal with it
  def registerAndEnqueueWorkFromTree(from : Path, copyTo : Path) : Unit = {
    val watchKey = tryRegister(from)

    val enqueueCopyMTime = watchKey match {
      // Might be a file, might have gone away. Either way, let's try a copy
      case None => Some(getMTime(from))
      case Some(_) if Files.isDirectory(copyTo) => {
        // Target path already exists: don't worry about making the directory + its contents appear atomically
        for (child <- Files.list(from).iterator().asScala) {
          registerAndEnqueueWorkFromTree(child, copyTo.resolve(from.relativize(child)))
        }
        None
      }
      // This seems to be a new directory: arrange for it to appear atomically. No need to copy children because that will be done by the dir copy.
      case Some(_) => Some(registerTreeChildren(from, getMTime(from)))
    }

    enqueueCopyMTime.foreach { mTime =>
      workQueue.add(Copy(mTime, from, copyTo))
    }
  }

  def registerAndEnqueueWorkFromTree(from : Path) : Unit = {
    registerAndEnqueueWorkFromTree(from, toRoot.resolve(fromRoot.relativize(from)))
  }

  def filesMayBeDifferent(from : Path, to : Path) = try {
    Files.size(to) != Files.size(from) || Files.isDirectory(to) != Files.isDirectory(from)
  } catch {
    case (_ : NoSuchFileException) => true
  }

  @volatile var seenAllFilesUpToMTime = Instant.now()
  registerAndEnqueueWorkFromTree(fromRoot, toRoot)

  val copyThread = new Thread {
    def copyChildren(from : Path, to : Path) : Unit = {
      for (fromChild <- Files.list(from).iterator().asScala) {
        val toChild = to.resolve(from.relativize(fromChild))
        if (Files.isDirectory(fromChild)) {
          logger.info(s"Recursively copying child directory $fromChild to $toChild")
          Files.createDirectory(toChild)
          copyChildren(fromChild, toChild)
        } else if (filesMayBeDifferent(fromChild, toChild)) {
          logger.info(s"Recursively copying child file $fromChild to $toChild")
          try {
            Files.copy(fromChild, toChild)
          } catch {
            case (e : NoSuchFileException) => logger.info(s"It appears that $fromChild has been deleted before we got a chance to copy it", e)
          }
        } else {
          logger.info(s"Skipping recursive copy of child file $fromChild to $toChild because we don't seem to have to do anything")
        }
      }
    }

    def deleteChildren(to : Path) : Unit = {
      for (toChild <- Files.list(to).iterator().asScala) {
        if (Files.isDirectory(toChild)) {
          logger.info(s"Recursively deleting child directory $toChild")
          deleteChildren(toChild)
        }
        Files.delete(toChild)
      }
    }

    override def run() = {
      while (true) {
        val workItem = workQueue.poll(5, TimeUnit.SECONDS)
        if (workItem != null) {
          val Copy(mTime, from, to) = workItem

          if (!mTime.isBefore(seenAllFilesUpToMTime)) {
            // There might be as-yet-undiscovered files with earlier MTimes on the file system, so wait for
            // more info -- we don't want to push files out-of-order!
            Thread.sleep(1000)
            workQueue.add(workItem)
          } else {
            if (!Files.isDirectory(to.getParent)) {
              // Parent directory does not exist: this happens *exactly* in the case where we've enqueued some work
              // via registerAndEnqueueWorkFromTree(Path) in a directory that is registered with the service but
              // where we have not yet processed the initial work item that copies the contents of that directory.
              //
              // In this situation we just ignore the work item because it will get picked up eventually be that recursive copy.
            } else {
              try {
                val toTemp = if (Files.isDirectory(from)) {
                  val toTemp = Files.createTempDirectory(to.getParent, ".filepusher." + from.getFileName.toString)
                  logger.info(s"Copying directory $from to $to via temporary $toTemp")
                  copyChildren(from, toTemp)
                  Some(toTemp)
                } else if (filesMayBeDifferent(from, to)) {
                  val toTemp = Files.createTempFile(to.getParent, ".filepusher." + from.getFileName.toString, "")
                  logger.info(s"Copying file $from to $to via temporary $toTemp")
                  try {
                    Files.copy(from, toTemp, StandardCopyOption.REPLACE_EXISTING)
                    Some(toTemp)
                  } catch {
                    case (e : NoSuchFileException) => {
                      logger.info(s"It appears that $from has been deleted before we got a chance to copy it", e)
                      None
                    }
                  }
                } else {
                  logger.info(s"Skipping copy from $from to $to because we don't seem to have to do anything")
                  None
                }
                toTemp.foreach { toTemp =>
                  if (Files.isDirectory(to)) {
                    logger.info(s"Non-atomically replacing directory $to with $toTemp (alas, it is not possible to atomically replace directories)")
                    deleteChildren(to)
                    Files.delete(to)
                    Files.move(toTemp, to)
                  } else {
                    logger.info(s"Atomically replacing $to with $toTemp")
                    Files.move(toTemp, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
                  }
                }
              } catch {
                case (e : FileNotFoundException) => logger.info("File went missing during recursive copy: never mind", e)
              }
            }
          }
        }
      }
    }
  }

  copyThread.setName("copy")
  copyThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
    override def uncaughtException(t : Thread, e : Throwable) = {
      logger.error(s"Thread ${t.getName} died, terminating VM!", e)
      System.exit(1)
    }
  })
  copyThread.start()

  while (true) {
    val key = {
      val key = watchService.poll()
      if (key != null) key else {
        // No other watch events are enqueued ==> we should have seen all files with MTimes up to now
        seenAllFilesUpToMTime = Instant.now().minus(WATCH_SERVICE_DELAY_SECONDS, ChronoUnit.SECONDS)
        watchService.poll(5, TimeUnit.SECONDS)
      }
    }
    if (key != null) {
      val path = key.watchable().asInstanceOf[Path]

      key.pollEvents().asScala.foreach { event =>
        logger.info(s"Got ${event.kind()} event for $path (context ${event.context()})")
        if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
          registerAndEnqueueWorkFromTree(path)
        } else {
          val childFilename = event.context().asInstanceOf[Path]
          val childPath = path.resolve(childFilename)
          // We ignore ENTRY_MODIFY events reported against directories that we are already watching because they just
          // mean that the contents of that directory were changed. Because we recursively subscribe to all directories
          // anyway, we will be able to spot that fact from the ENTRY_MODIFY/ENTRY_CREATE for the actual nested file or
          // unmonitored subdirectory that has just changed.
          //
          // We have to be careful that we don't ignore the event in the case where childPath has just been deleted and
          // replaced with a file, this is done by checking the isValid flag of the registered key.
          val isDirectoryWeAreAlreadyWatching = isRegistered.contains(childPath) && isRegistered(childPath).isValid
          if (!isDirectoryWeAreAlreadyWatching || event.kind() != StandardWatchEventKinds.ENTRY_MODIFY) {
            registerAndEnqueueWorkFromTree(childPath)
          }
        }
      }

      // The key may be re-queued in the WatchService only if we reset it
      if (!key.reset()) {
        // The key has been auto-cancelled (probably because the file is now dead)
        isRegistered.remove(path)
      }
    }
  }

  // Work must be drained in an order, <:, compatible with the following axioms:
  //  1. If A and B are files where mtime(A) < mtime(B) then A <: B
  //  2. If A is strictly contained within B, and generation(A) <= generation(B), then A <: B
  //
  // Generation is a number assigned to each work item:
  //  1. All the work added as part of a single trawl is assigned to the same generation
  //  2. Generation number increments after each trawl
  //
  // NB: work is defined as:
  //  a) File work: atomically replace the "to"-space file with the "from"-space one
  //  b) Directory work: atomically create a "from"-space directory containing the initial contents of the "to"-space directory
}
