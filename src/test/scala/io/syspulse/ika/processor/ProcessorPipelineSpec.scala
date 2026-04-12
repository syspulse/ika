package io.syspulse.ika.processor

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import org.scalatest.concurrent.ScalaFutures

class ProcessorPipelineSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global

  // Test processors
  class SetDestinationProcessor(dest: String) extends RequestProcessor {
    def name: String = "SetDestination"
    def processRequest(session: Session): Future[Session] = {
      Future.successful(session.putData("destination", dest))
    }
  }

  class AddHeaderProcessor(key: String, value: String) extends RequestProcessor {
    def name: String = "AddHeader"
    def processRequest(session: Session): Future[Session] = {
      Future.successful(session.putData(key, value))
    }
  }

  class RejectingProcessor extends RequestProcessor {
    def name: String = "Rejecting"
    def processRequest(session: Session): Future[Session] = {
      Future.successful(session.reject(400, "Rejected", "Rejecting"))
    }
  }

  class FailingProcessor extends RequestProcessor {
    def name: String = "Failing"
    def processRequest(session: Session): Future[Session] = {
      Future.failed(new Exception("Processor failed"))
    }
  }

  class CountingProcessor extends RequestProcessor {
    var callCount = 0
    def name: String = "Counting"
    def processRequest(session: Session): Future[Session] = {
      callCount += 1
      Future.successful(session.putData("count", callCount))
    }
  }

  "ProcessorPipeline" should {

    "execute processors in sequence" in {
      val pipeline = ProcessorPipeline(
        new SetDestinationProcessor("http://localhost:8545"),
        new AddHeaderProcessor("header1", "value1"),
        new AddHeaderProcessor("header2", "value2")
      )

      val session = Session(requestBody = "test")
      val result = Await.result(pipeline.process(session), 5.seconds)

      result.getData[String]("destination") shouldBe Some("http://localhost:8545")
      result.getData[String]("header1") shouldBe Some("value1")
      result.getData[String]("header2") shouldBe Some("value2")
    }

    "short-circuit on rejection" in {
      val counter = new CountingProcessor()
      val pipeline = ProcessorPipeline(
        new AddHeaderProcessor("before", "rejection"),
        new RejectingProcessor(),
        counter // This should not be called
      )

      val session = Session(requestBody = "test")
      val result = Await.result(pipeline.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.message shouldBe "Rejected"
      result.getData[String]("before") shouldBe Some("rejection")
      counter.callCount shouldBe 0 // Should not have been called
    }

    "recover from processor failures" in {
      val pipeline = ProcessorPipeline(
        new AddHeaderProcessor("before", "failure"),
        new FailingProcessor(),
        new AddHeaderProcessor("after", "failure") // Should not be called
      )

      val session = Session(requestBody = "test")
      val result = Await.result(pipeline.process(session), 5.seconds)

      result.isRejected shouldBe true
      result.rejection.get.code shouldBe -32603
      result.rejection.get.message should include("Internal processor error")
      result.rejection.get.processorName shouldBe "Failing"
      result.getData[String]("before") shouldBe Some("failure")
      result.getData[String]("after") shouldBe None
    }

    "mark session as complete" in {
      val pipeline = ProcessorPipeline(
        new SetDestinationProcessor("http://localhost:8545")
      )

      val session = Session(requestBody = "test")
      val result = Await.result(pipeline.process(session), 5.seconds)

      result.endTime shouldBe defined
      result.durationMs should be >= 0L
    }

    "handle empty pipeline" in {
      val pipeline = new ProcessorPipeline(Seq.empty)

      val session = Session(requestBody = "test")
      val result = Await.result(pipeline.process(session), 5.seconds)

      result.isRejected shouldBe false
      result.endTime shouldBe defined
    }

    "propagate session state through pipeline" in {
      val pipeline = ProcessorPipeline(
        new SetDestinationProcessor("http://localhost:8545"),
        new AddHeaderProcessor("step1", "done"),
        new AddHeaderProcessor("step2", "done"),
        new AddHeaderProcessor("step3", "done")
      )

      val session = Session(requestBody = "test").putData("maxRetry", 5)
      val result = Await.result(pipeline.process(session), 5.seconds)

      result.getData[String]("destination") shouldBe Some("http://localhost:8545")
      result.getData[Int]("maxRetry") shouldBe Some(5)
      result.getData[String]("step1") shouldBe Some("done")
      result.getData[String]("step2") shouldBe Some("done")
      result.getData[String]("step3") shouldBe Some("done")
    }

    "have meaningful toString" in {
      val pipeline = ProcessorPipeline(
        "TestPipeline",
        new SetDestinationProcessor("http://localhost:8545"),
        new AddHeaderProcessor("header", "value")
      )

      pipeline.toString should include("TestPipeline")
      pipeline.toString should include("SetDestination")
      pipeline.toString should include("AddHeader")
      pipeline.toString should include("→")
    }
  }

  "ProcessorPipeline object" should {

    "create pipeline from varargs" in {
      val pipeline = ProcessorPipeline(
        new SetDestinationProcessor("http://localhost:8545")
      )

      pipeline.processors.size shouldBe 1
    }

    "create named pipeline from varargs" in {
      val pipeline = ProcessorPipeline(
        "MyPipeline",
        new SetDestinationProcessor("http://localhost:8545")
      )

      pipeline.name shouldBe "MyPipeline"
    }

    "create pipeline from sequence" in {
      val processors = Seq(
        new SetDestinationProcessor("http://localhost:8545"),
        new AddHeaderProcessor("header", "value")
      )
      val pipeline = ProcessorPipeline.fromSeq(processors, "SeqPipeline")

      pipeline.processors.size shouldBe 2
      pipeline.name shouldBe "SeqPipeline"
    }
  }
}
