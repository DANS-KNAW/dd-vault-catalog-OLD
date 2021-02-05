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
import scala.collection.mutable.ListBuffer

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

      val update_catalog = "INSERT INTO catalog(dataverse_pid, dataverse_pid_version, bag_id, nbn,bag_file_path, depositor, title) " +
        "VALUES(?,?,?,?,?,?,?)"
      val catalog_stmt = conn.prepareStatement(update_catalog)

      val update_ocfl = "INSERT INTO ocfl_version(metadata, object_version_checksum, object_version_deposit_date, bag_id, object_version, object_version_file_path) " +
        "VALUES(?,?,?,?,?,?)"
      val ocfl_stmt = conn.prepareStatement(update_ocfl)

      catalog_stmt.setString(1, dataverse_pid)
      catalog_stmt.setString(2, dataverse_pid_version)
      catalog_stmt.setString(3, bag_id)
      catalog_stmt.setString(4, nbn)
      catalog_stmt.setString(5, bag_file_path)
      catalog_stmt.setString(6, depositor)
      catalog_stmt.setString(7, title)

      ocfl_stmt.setString(1, metadata)
      ocfl_stmt.setString(2, object_version_checksum)
      ocfl_stmt.setDate(3, new java.sql.Date(System.currentTimeMillis()))
      ocfl_stmt.setString(4, bag_id)
      ocfl_stmt.setString(5, "")
      ocfl_stmt.setString(6, bag_file_path)

      catalog_stmt.executeUpdate()
      ocfl_stmt.executeUpdate()

      //TODO return proper status codes

    }

  }

  get("/catalog") {
    contentType = "application/json"

    classOf[org.postgresql.Driver]
    val con_str = "jdbc:postgresql://localhost:5433/dv2tape?user=postgres"
    val conn = DriverManager.getConnection(con_str)
    var catalog_output = new ListBuffer[JObject]()
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = stm.executeQuery("SELECT * from catalog NATURAL JOIN ocfl_version")


      while (rs.next) {

        val catalog = Catalog(rs.getString("dataverse_pid"), rs.getString("dataverse_pid_version"), rs.getString("bag_id"), rs.getString("nbn"), rs.getString("bag_file_path"), rs.getString("depositor"), rs.getString("title"))
        val ocfl = OcflVersion(rs.getString("metadata"), rs.getString("object_version_checksum"), rs.getDate("object_version_deposit_date"), rs.getString("bag_id"), rs.getString("object_version"), rs.getString("object_version_file_path"))
        json =
          ("archived_dataset" ->
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
        catalog_output.append(json)
        //println(pretty(render(json)))
      }
    } finally {
      conn.close()
    }
    response.getOutputStream
    //TODO fix json return body, nothing returned
    for (json <- catalog_output){
      json
      //pretty(render(json))
      //println(pretty(render(json)))
    }

  }


}


