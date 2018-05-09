import java.text.SimpleDateFormat

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient

import scala.util.control.NonFatal

object testHttpJson {
  def main(args: Array[String]): Unit = {

    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val currentTime = df.format(System.currentTimeMillis())
    val body: String =
      f"""{
         |        "uid": "110",
         |        "name": "Roy#",
         |        "password": "123"
         |}""".stripMargin

    println(body)
    val url = "http://localhost:18080"
    try {
      val httpClient = new DefaultHttpClient()
      val post = new HttpPost(url)
      post.setHeader("Content-type", "application/json")
      post.setEntity(new StringEntity(body))
      val response = httpClient.execute(post)

    } catch {
      case NonFatal(t) => {
        System.out.println(t)
      }
    }
  }
}
