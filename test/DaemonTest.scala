import java.lang.Thread.UncaughtExceptionHandler
import java.nio.file.{Paths, Path, Files}
import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant}

import org.scalatest.FunSuite

import scala.collection.mutable
import scala.util.Random

class DaemonTest extends FunSuite {
  trait Helper {
    def start()
    def quiesce()
    def assertEventually(pred : => Boolean)
    def remove(path : String)
    def touch(path : String, cookie : Int = 0)
    def fromPath(path : String) : Path
    def toPath(path : String) : Path
  }

  def withDaemon[A](act : Helper => A) = {
    val root = Files.createTempDirectory("daemon-test")
    val fromRoot = root.resolve("from")
    val toRoot   = root.resolve("to")
    Files.createDirectory(fromRoot)
    @volatile var hasStarted = false
    val daemonThread = new Thread {
      override def run() = new Daemon(fromRoot, toRoot)
    }
    daemonThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
      override def uncaughtException(t : Thread, e : Throwable) : Unit = e match {
        case (_ : InterruptedException) => () // Sure, whatever - this just means we are killing the thread
        case _ => {
          e.printStackTrace()
          System.exit(1)
        }
      }
    })

    val helper = new Helper {
      override def start() : Unit = if (!hasStarted) {
        hasStarted = true
        daemonThread.start()
      }
      override def quiesce() = Thread.sleep(20 * 1000) // Very cheap and cheerful...
      override def assertEventually(pred : => Boolean) = {
        val limit = Instant.now().plus(20, ChronoUnit.SECONDS)
        while (Instant.now().isBefore(limit) && !pred) {
          Thread.sleep(100)
        }
        assert(pred)
      }
      override def remove(path: String) : Unit = Files.delete(fromRoot.resolve(path))
      override def touch(path: String, cookie : Int) : Unit = {
        val where = fromRoot.resolve(path)
        Files.createDirectories(where.getParent)
        Files.write(where, Array.ofDim[Byte](cookie))
      }
      override def fromPath(path : String) = fromRoot.resolve(path)
      override def toPath(path : String) = toRoot.resolve(path)
    }
    try {
      act(helper)
    } finally {
      if (hasStarted) {
        daemonThread.interrupt()
        daemonThread.join(10 * 1000)
        if (daemonThread.isAlive) throw new IllegalStateException("The daemon should have shut down..")
      }
    }
  }

  test("Can copy directory") { withDaemon { d =>
    d.start()
    d.quiesce()
    d.touch("foo/bar")
    d.assertEventually { Files.isRegularFile(d.toPath("foo/bar")) }
  }}

  test("Can copy file") { withDaemon { d =>
    d.start()
    d.quiesce()
    d.touch("foo")
    d.assertEventually { Files.isRegularFile(d.toPath("foo")) }
  }}

  test("Can replace file with file") { withDaemon { d =>
    d.touch("foo")
    d.start()
    d.assertEventually { Files.isRegularFile(d.toPath("foo")) }
    d.touch("foo", cookie=1)
    d.assertEventually { Files.size(d.toPath("foo")) == 1 }
  }}

  test("Can replace file with directory") { withDaemon { d =>
    d.touch("foo")
    d.start()
    d.remove("foo")
    d.touch("foo/bar")
    d.assertEventually { Files.isRegularFile(d.toPath("foo/bar")) }
  }}

  test("Can replace directory with file") { withDaemon { d =>
    d.touch("foo/bar")
    d.start()
    d.remove("foo/bar")
    d.remove("foo")
    d.touch("foo")
    d.assertEventually { Files.isRegularFile(d.toPath("foo")) }
  }}

  test("Can cope with random series of file ops") { withDaemon { d =>
    val random = new Random()

    def choose[A](from : TraversableOnce[A], satisfying : A => Boolean) = random.shuffle(from).find(satisfying)
      //.getOrElse(throw new IllegalStateException("No possibilty satisfied the predicate"))

    d.start()
    d.quiesce()

    // Mutable model of what we believe the FS should contain
    val directories = new mutable.ArrayBuffer[String]()
    val files = new mutable.HashMap[String, Int]()
    directories += "."

    def isEmptyDirectory(dir : String) = !directories.exists(_.startsWith(dir)) && !files.keys.exists(_.startsWith(dir))

    // We deliberately choose a child path that is likely to collide with one we already created, because
    // that results in maximally mind-bending situations for the daemon to cope with
    def chooseChild() = choose(directories, (_ : String) => true).map { x =>
      var i = 0
      while (Files.exists(d.fromPath(x).resolve("child" + i))) {
        i += 1
      }
      d.fromPath(x).resolve("child" + i)
    }

    // Let's say we want do to 30s of random ops. We wait 50ms between each op, so we need to do 600 ops on average.
    var cookie = 0
    while (random.nextFloat() < 1f - (1 / 1200f)) {
      Thread.sleep(50)
      random.nextInt(4) match {
        case 0 => choose(directories, isEmptyDirectory).foreach { x =>
          directories -= x
          Files.delete(d.fromPath(x))
        }
        case 1 => choose(files, (_ : (String, Int)) => true).foreach { case (x, _) =>
          files.remove(x)
          Files.delete(d.fromPath(x))
        }
        case 2 => chooseChild().foreach { p =>
          directories += p.toString
          Files.createDirectory(p)
        }
        case 3 => chooseChild().foreach { p =>
          files.put(p.toString, cookie)
          Files.write(p, Array.ofDim[Byte](cookie))
          cookie += 1
        }
      }
    }

    d.assertEventually {
      directories.forall(x => Files.isDirectory(d.toPath(x)))
      files.forall { case (x, sz) => Files.size(d.toPath(x)) == sz }
    }
  }}
}
