/**
 * Copyright (C) 2021 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.dd.catalog

import com.fasterxml.jackson.databind.ObjectMapper
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods
import org.json4s.native.JsonMethods.{pretty, render}
import org.json4s.{DefaultFormats, Formats, JsonAST}
import org.scalatra._
import com.fasterxml.jackson.databind.JsonNode
import nl.knaw.dans.dd.catalog.Command.app
import org.json4s.JsonAST.JObject
import org.json4s.scalap.scalasig.ClassFileParser._
import org.json4s.scalap.~

import java.nio.charset.StandardCharsets
import scala.collection.JavaConversions._
import java.sql.{DriverManager, ResultSet, Statement}

class DdVaultCatalogServlet(app: DdVaultCatalogApp,
                            version: String) extends ScalatraServlet with DebugEnhancedLogging {

  private implicit val jsonFormats: Formats = DefaultFormats

  //val con_str: String = new String (app.config.dbUrl + app.config.dbPort + "/" + app.config.dbName + "?user=" + app.config.dbUser + "&password=" + app.config.dbPassword)
  var dataverse_pid: String = ""
  var dataverse_pid_version: String = ""
  var bag_id: String = ""
  var nbn: String = ""
  var bag_file_path: String = "/BagOutbox/Datastation/"
  var depositor: String = ""
  var title: String = ""

  var metadata: String = ""
  var object_version_checksum = ""

  var json: JObject = _

  case class Catalog(dataverse_pid: String, dataverse_pid_version: String, bag_id: String, nbn: String, bag_file_path: String, depositor: String, title: String)

  case class OcflVersion(metadata: String, object_version_checksum: String, object_version_deposit_date: java.sql.Date, bag_id: String, object_version: String, object_version_file_path: String)

  get("/") {
    contentType = "text/plain"
    Ok(s"DD Vault Catalog Service running ($version)")
  }

  post("/json-to-catalog") {
    contentType = "application/json"
    val objectMapper = new ObjectMapper()
    val rootNode = objectMapper.readTree(request.body)
    val dataVaultFields = rootNode.get("data").get("metadataBlocks").get("dansDataVaultMetadata").get("fields")
    val citationFields = rootNode.get("data").get("metadataBlocks").get("citation").get("fields")

    metadata = new String(request.body)

    for (f <- dataVaultFields) {
      if (f.get("typeName").asText == "dansDataversePid") dataverse_pid = f.get("value").asText
      if (f.get("typeName").asText == "dansDataversePidVersion") dataverse_pid_version = f.get("value").asText
      if (f.get("typeName").asText == "dansBagId") bag_id = f.get("value").asText
      if (f.get("typeName").asText == "dansNbn") nbn = f.get("value").asText
    }

    for (f <- citationFields) {
      if (f.get("typeName").asText == "depositor") depositor = f.get("value").asText
      if (f.get("typeName").asText == "title") title = f.get("value").asText
    }

    bag_file_path = bag_file_path + bag_id + ".zip"

    val con_str = "jdbc:postgresql://localhost:5433/dv2tape?user=postgres"
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(con_str)

    try {
      val stm = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)

      {
        val rs = stm.executeQuery("SELECT * FROM catalog")
        val rs2 = stm.executeQuery("SELECT * FROM ocfl_version")
        rs.moveToInsertRow()
        rs.updateString("dataverse_pid", dataverse_pid)
        rs.updateString("dataverse_pid_version", dataverse_pid_version)
        rs.updateString("bag_id", bag_id)
        rs.updateString("nbn", nbn)
        rs.updateString("bag_file_path", bag_file_path)
        rs.updateString("depositor", depositor)
        rs.updateString("title", title)

        rs2.moveToInsertRow()
        rs2.updateString("metadata", metadata)
        rs2.updateString("object_version_checksum", object_version_checksum)
        rs2.updateDate("object_version_deposit_date", new java.sql.Date(System.currentTimeMillis()))
        rs2.updateString("bag_id", bag_id)
        rs2.updateString("object_version", "")
        rs2.updateString("object_version_file_path", bag_file_path)

        rs.insertRow()
        rs.beforeFirst()

        rs2.insertRow()
        rs2.beforeFirst()
      }
    }
  }

  get("/catalog") {
    contentType = "application/json"
    println("Postgres connector")

    classOf[org.postgresql.Driver]
    val con_str = "jdbc:postgresql://localhost:5433/dv2tape?user=postgres"
    val conn = DriverManager.getConnection(con_str)
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = stm.executeQuery("SELECT * from catalog NATURAL JOIN ocfl_version")

      while (rs.next) {

        val catalog = Catalog(rs.getString("dataverse_pid"), rs.getString("dataverse_pid_version"), rs.getString("bag_id"), rs.getString("nbn"), rs.getString("bag_file_path"), rs.getString("depositor"), rs.getString("title"))
        val ocfl = OcflVersion(rs.getString("metadata"),rs.getString("object_version_checksum"),rs.getDate("object_version_deposit_date"),rs.getString("bag_id"),rs.getString("object_version"),rs.getString("object_version_file_path"))
        json =
            ("catalog" ->
            ("dataverse_pid" -> catalog.dataverse_pid) ~
              ("dataverse_pid_version" -> catalog.dataverse_pid_version) ~
              ("bag_id" -> catalog.bag_id) ~
              ("nbn" -> catalog.nbn) ~
              ("bag_file_path" -> catalog.bag_file_path) ~
              ("depositor" -> catalog.depositor) ~
              ("title" -> catalog.title) ~
              ("metadata" -> ocfl.metadata) ~
              ("object_version_checksum" -> ocfl.object_version_checksum) ~
              ("object_version_deposit_date" -> ocfl.object_version_deposit_date.toString) ~
              ("bag_id" -> ocfl.bag_id) ~
              ("object_version" -> ocfl.object_version) ~
              ("object_version_file_path" -> ocfl.object_version_file_path)
            )

        println(pretty(render(json)))
      }
    } finally {
      conn.close()
    }
    pretty(render(json))
  }


}


