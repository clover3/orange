package common

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}


package object future {
  def serialiseFutures[A, B](l: Iterable[A])(fn: A => Future[B])
                            (implicit ec: ExecutionContext): Future[List[B]] = {
    l.foldLeft(Future(List.empty[B])) {
      (previousFuture, next) =>
        for {
          previousResults <- previousFuture
          next <- fn(next)
        } yield previousResults :+ next
    }
  }

  def all[T](fs: List[Future[T]])(implicit ec: ExecutionContext): Future[List[T]] = {
    val p = Promise[List[T]]()
    fs match {
      case head::tail => {
        head onComplete{
          case Failure(e) => p.failure(e)
          case Success(x) => all(tail) onComplete {
            case Failure(e) => p.failure(e)
            case Success(y) => p.success(x :: y)
          }
        }
      }
      case Nil => p.success(Nil)
    }
    p.future
  }
}