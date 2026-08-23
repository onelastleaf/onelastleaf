package example

object Echo:
  def run(arguments: Seq[String]): String = arguments.mkString(" ")
